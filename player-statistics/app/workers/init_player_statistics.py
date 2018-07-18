import json

from aioamqp import AmqpClosedConnection
from bson import ObjectId
from marshmallow import ValidationError
from marshmallow.utils import missing
from sanic_amqp_ext import AmqpWorker
from sage_utils.constants import VALIDATION_ERROR
from sage_utils.wrappers import Response


class InitPlayerStatisticsWorker(AmqpWorker):
    QUEUE_NAME = 'player-stats.statistic.init'
    REQUEST_EXCHANGE_NAME = 'open-matchmaking.player-stats.statistic.init.direct'
    RESPONSE_EXCHANGE_NAME = 'open-matchmaking.responses.direct'
    CONTENT_TYPE = 'application/json'

    def __init__(self, app, *args, **kwargs):
        super(InitPlayerStatisticsWorker, self).__init__(app, *args, **kwargs)
        from app.statistics.documents import PlayerStatistic
        from app.statistics.schemas import InitPlayerStatisticSchema
        self.player_statistic_document = PlayerStatistic
        self.schema = InitPlayerStatisticSchema

    async def validate_data(self, raw_data):
        try:
            data = json.loads(raw_data.strip())
        except json.decoder.JSONDecodeError:
            data = {}

        deserializer = self.schema()
        result = deserializer.load(data)
        if result.errors:
            raise ValidationError(result.errors)

        return result.data

    async def init_player_statistic(self, raw_data):
        try:
            data = await self.validate_data(raw_data)
        except ValidationError as exc:
            return Response.from_error(VALIDATION_ERROR, exc.normalized_messages())

        await self.player_statistic_document.collection.replace_one(
            {'player_id': data['player_id']}, replacement=data, upsert=True
        )
        document = await self.player_statistic_document.find_one(
            {'player_id': data['player_id']}
        )

        return Response.with_content(document.dump())

    async def process_request(self, channel, body, envelope, properties):
        response = await self.init_player_statistic(body)
        response.data[Response.EVENT_FIELD_NAME] = properties.correlation_id

        if properties.reply_to:
            await channel.publish(
                json.dumps(response.data),
                exchange_name=self.RESPONSE_EXCHANGE_NAME,
                routing_key=properties.reply_to,
                properties={
                    'content_type': self.CONTENT_TYPE,
                    'delivery_mode': 2,
                    'correlation_id': properties.correlation_id
                },
                mandatory=True
            )

        await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

    async def consume_callback(self, channel, body, envelope, properties):
        self.app.loop.create_task(self.process_request(channel, body, envelope, properties))

    async def run(self, *args, **kwargs):
        try:
            _transport, protocol = await self.connect()
        except AmqpClosedConnection as exc:
            print(exc)
            return

        channel = await protocol.channel()
        await channel.queue_declare(
            queue_name=self.QUEUE_NAME,
            durable=True,
            passive=False,
            auto_delete=False
        )
        await channel.queue_bind(
            queue_name=self.QUEUE_NAME,
            exchange_name=self.REQUEST_EXCHANGE_NAME,
            routing_key=self.QUEUE_NAME
        )
        await channel.basic_qos(prefetch_count=1, prefetch_size=0, connection_global=False)
        await channel.basic_consume(self.consume_callback, queue_name=self.QUEUE_NAME)
