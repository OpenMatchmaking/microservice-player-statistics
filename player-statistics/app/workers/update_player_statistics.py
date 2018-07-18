import json

from aioamqp import AmqpClosedConnection
from bson import ObjectId
from marshmallow import ValidationError
from sanic_amqp_ext import AmqpWorker
from sage_utils.constants import VALIDATION_ERROR, NOT_FOUND_ERROR
from sage_utils.wrappers import Response


class UpdatePlayerStatisticsWorker(AmqpWorker):
    QUEUE_NAME = 'player-stats.statistic.update'
    REQUEST_EXCHANGE_NAME = 'open-matchmaking.player-stats.statistic.update.direct'
    RESPONSE_EXCHANGE_NAME = 'open-matchmaking.responses.direct'
    CONTENT_TYPE = 'application/json'

    PLAYER_NOT_FOUND_ERROR = "Player was not found or doesn't exist."

    def __init__(self, app, *args, **kwargs):
        super(UpdatePlayerStatisticsWorker, self).__init__(app, *args, **kwargs)
        from app.statistics.documents import PlayerStatistic
        from app.statistics.schemas import UpdatePlayerStatisticSchema
        self.player_statistic_document = PlayerStatistic
        self.schema = UpdatePlayerStatisticSchema

    async def validate_data(self, raw_data):
        try:
            data = json.loads(raw_data.strip())
        except json.decoder.JSONDecodeError:
            data = {}

        player_id = ObjectId(data['player_id']) if 'player_id' in data else None
        document = await self.player_statistic_document.find_one({'player_id': player_id})
        if document is None:
            raise ValueError()

        return document, data

    async def update_player_statistic(self, raw_data):
        try:
            document, data = await self.validate_data(raw_data)
            document.update(data)
            await document.commit()
        except ValidationError as exc:
            return Response.from_error(VALIDATION_ERROR, exc.normalized_messages())
        except ValueError:
            return Response.from_error(NOT_FOUND_ERROR, self.PLAYER_NOT_FOUND_ERROR)

        return Response.with_content(document.dump())

    async def process_request(self, channel, body, envelope, properties):
        response = await self.update_player_statistic(body)
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
        print('done')
