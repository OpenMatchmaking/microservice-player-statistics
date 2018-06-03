import pytest
from bson import ObjectId
from sage_utils.amqp.clients import RpcAmqpClient
from sage_utils.constants import NOT_FOUND_ERROR
from sage_utils.wrappers import Response

from app.statistics.documents import PlayerStatistic
from app.workers.retrieve_player_statistics import RetrievePlayerStatisticsWorker


REQUEST_QUEUE = RetrievePlayerStatisticsWorker.QUEUE_NAME
REQUEST_EXCHANGE = RetrievePlayerStatisticsWorker.REQUEST_EXCHANGE_NAME
RESPONSE_EXCHANGE = RetrievePlayerStatisticsWorker.RESPONSE_EXCHANGE_NAME
PLAYER_NOT_FOUND_ERROR = RetrievePlayerStatisticsWorker.PLAYER_NOT_FOUND_ERROR


@pytest.mark.asyncio
async def test_worker_returns_a_player_statistics(sanic_server):
    await PlayerStatistic.collection.delete_many({})

    player_id = str(ObjectId())
    create_data = {
        'player_id': player_id,
        'total_games': 10,
        'wins': 6,
        'loses': 4,
        'rating': 2676
    }
    object = PlayerStatistic(**create_data)
    await object.commit()

    players_count = await PlayerStatistic.collection.find().count()
    assert players_count == 1

    retrieve_data = {'player_id': player_id}
    client = RpcAmqpClient(
        sanic_server.app,
        routing_key=REQUEST_QUEUE,
        request_exchange=REQUEST_EXCHANGE,
        response_queue='',
        response_exchange=RESPONSE_EXCHANGE
    )
    response = await client.send(payload=retrieve_data)

    assert Response.EVENT_FIELD_NAME in response.keys()
    assert Response.CONTENT_FIELD_NAME in response.keys()
    content = response[Response.CONTENT_FIELD_NAME]

    assert len(list(content.keys())) == 6
    assert set(content.keys()) == {'id', 'player_id', 'total_games', 'wins', 'loses', 'rating'}

    assert content['player_id'] == create_data['player_id']
    assert content['total_games'] == create_data['total_games']
    assert content['wins'] == create_data['wins']
    assert content['loses'] == create_data['loses']
    assert content['rating'] == create_data['rating']

    await PlayerStatistic.collection.delete_many({})


@pytest.mark.asyncio
async def test_worker_returns_a_not_found_error(sanic_server):
    await PlayerStatistic.collection.delete_many({})

    retrieve_data = {'player_id': str(ObjectId())}
    client = RpcAmqpClient(
        sanic_server.app,
        routing_key=REQUEST_QUEUE,
        request_exchange=REQUEST_EXCHANGE,
        response_queue='',
        response_exchange=RESPONSE_EXCHANGE
    )
    response = await client.send(payload=retrieve_data)

    assert Response.ERROR_FIELD_NAME in response.keys()
    error = response[Response.ERROR_FIELD_NAME]

    assert Response.ERROR_TYPE_FIELD_NAME in error.keys()
    assert error[Response.ERROR_TYPE_FIELD_NAME] == NOT_FOUND_ERROR

    assert Response.ERROR_DETAILS_FIELD_NAME in error.keys()
    assert isinstance(error[Response.ERROR_DETAILS_FIELD_NAME], str)
    assert error[Response.ERROR_DETAILS_FIELD_NAME] == PLAYER_NOT_FOUND_ERROR

    await PlayerStatistic.collection.delete_many({})
