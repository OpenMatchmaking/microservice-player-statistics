import pytest
from sage_utils.amqp.clients import RpcAmqpClient
from sage_utils.constants import VALIDATION_ERROR
from sage_utils.wrappers import Response

from app.statistics.documents import PlayerStatistic
from app.workers.init_player_statistics import InitPlayerStatisticsWorker


REQUEST_QUEUE = InitPlayerStatisticsWorker.QUEUE_NAME
REQUEST_EXCHANGE = InitPlayerStatisticsWorker.REQUEST_EXCHANGE_NAME
RESPONSE_EXCHANGE = InitPlayerStatisticsWorker.RESPONSE_EXCHANGE_NAME


# @pytest.mark.asyncio
# async def test_worker_returns_one_existing_server_for_one_server_in_list(sanic_server):
#     await PlayerStatistic.collection.delete_many({})
#
#     create_data = {
#         'host': '127.0.0.1',
#         'port': 9000,
#         'available_slots': 100,
#         'credentials': {
#             'token': 'super_secret_token'
#         },
#         'game_mode': '1v1'
#     }
#     objects = await create_game_servers([create_data, ])
#
#     client = RpcAmqpClient(
#         sanic_server.app,
#         routing_key=REQUEST_QUEUE,
#         request_exchange=REQUEST_EXCHANGE,
#         response_queue='',
#         response_exchange=RESPONSE_EXCHANGE
#     )
#     response = await client.send(payload={
#         'required_slots': 20,
#         'game_mode': "1v1"
#     })
#
#     assert Response.EVENT_FIELD_NAME in response.keys()
#     assert Response.CONTENT_FIELD_NAME in response.keys()
#     content = response[Response.CONTENT_FIELD_NAME]
#
#     assert len(list(content.keys())) == 3
#     assert set(content.keys()) == {'host', 'port', 'credentials'}
#
#     assert content['host'] == objects[0].host
#     assert content['port'] == objects[0].port
#     assert content['credentials'] == objects[0].credentials
#
#     await PlayerStatistic.collection.delete_many({})


@pytest.mark.asyncio
async def test_worker_returns_validation_for_non_specified_fields(sanic_server):
    await PlayerStatistic.collection.delete_many({})

    client = RpcAmqpClient(
        sanic_server.app,
        routing_key=REQUEST_QUEUE,
        request_exchange=REQUEST_EXCHANGE,
        response_queue='',
        response_exchange=RESPONSE_EXCHANGE
    )
    response = await client.send(payload={})

    assert Response.ERROR_FIELD_NAME in response.keys()
    error = response[Response.ERROR_FIELD_NAME]

    assert Response.ERROR_TYPE_FIELD_NAME in error.keys()
    assert error[Response.ERROR_TYPE_FIELD_NAME] == VALIDATION_ERROR

    assert Response.ERROR_DETAILS_FIELD_NAME in error.keys()
    assert len(error[Response.ERROR_DETAILS_FIELD_NAME]) == 2

    for field in ['required_slots', 'game_mode']:
        assert field in error[Response.ERROR_DETAILS_FIELD_NAME]
        assert len(error[Response.ERROR_DETAILS_FIELD_NAME][field]) == 1
        assert error[Response.ERROR_DETAILS_FIELD_NAME][field][0] == 'Missing data for ' \
                                                                     'required field.'

    players_count = await PlayerStatistic.collection.find().count()
    assert players_count == 0

    await PlayerStatistic.collection.delete_many({})