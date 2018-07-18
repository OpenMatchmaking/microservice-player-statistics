import pytest
from bson import ObjectId
from sage_utils.amqp.clients import RpcAmqpClient
from sage_utils.constants import VALIDATION_ERROR
from sage_utils.wrappers import Response

from app.statistics.documents import PlayerStatistic
from app.workers.init_player_statistics import InitPlayerStatisticsWorker


REQUEST_QUEUE = InitPlayerStatisticsWorker.QUEUE_NAME
REQUEST_EXCHANGE = InitPlayerStatisticsWorker.REQUEST_EXCHANGE_NAME
RESPONSE_EXCHANGE = InitPlayerStatisticsWorker.RESPONSE_EXCHANGE_NAME


@pytest.mark.asyncio
async def test_worker_returns_a_new_initialized_player(sanic_server):
    await PlayerStatistic.collection.delete_many({})

    player_id = str(ObjectId())
    init_data = {'player_id': player_id}
    client = RpcAmqpClient(
        sanic_server.app,
        routing_key=REQUEST_QUEUE,
        request_exchange=REQUEST_EXCHANGE,
        response_queue='',
        response_exchange=RESPONSE_EXCHANGE
    )
    response = await client.send(payload=init_data)

    assert Response.EVENT_FIELD_NAME in response.keys()
    assert Response.CONTENT_FIELD_NAME in response.keys()
    content = response[Response.CONTENT_FIELD_NAME]

    assert len(list(content.keys())) == 6
    assert set(content.keys()) == {'id', 'player_id', 'total_games', 'wins', 'loses', 'rating'}

    assert content['player_id'] == player_id
    assert content['total_games'] == 0
    assert content['wins'] == 0
    assert content['loses'] == 0
    assert content['rating'] == 0

    players_count = await PlayerStatistic.collection.count_documents({})
    assert players_count == 1

    await PlayerStatistic.collection.delete_many({})


@pytest.mark.asyncio
async def test_worker_returns_an_reinitialized_player(sanic_server):
    await PlayerStatistic.collection.delete_many({})

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

    players_count = await PlayerStatistic.collection.count_documents({})
    assert players_count == 1

    init_data = {'player_id': player_id}
    client = RpcAmqpClient(
        sanic_server.app,
        routing_key=REQUEST_QUEUE,
        request_exchange=REQUEST_EXCHANGE,
        response_queue='',
        response_exchange=RESPONSE_EXCHANGE
    )
    response = await client.send(payload=init_data)

    assert Response.EVENT_FIELD_NAME in response.keys()
    assert Response.CONTENT_FIELD_NAME in response.keys()
    content = response[Response.CONTENT_FIELD_NAME]

    assert len(list(content.keys())) == 6
    assert set(content.keys()) == {'id', 'player_id', 'total_games', 'wins', 'loses', 'rating'}

    assert content['player_id'] == create_data['player_id']
    assert content['player_id'] == player_id

    assert content['total_games'] != create_data['total_games']
    assert content['total_games'] == 0

    assert content['wins'] != create_data['wins']
    assert content['wins'] == 0

    assert content['loses'] != create_data['loses']
    assert content['loses'] == 0

    assert content['rating'] != create_data['rating']
    assert content['rating'] == 0

    players_count = await PlayerStatistic.collection.count_documents({})
    assert players_count == 1

    await PlayerStatistic.collection.delete_many({})


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
    assert len(error[Response.ERROR_DETAILS_FIELD_NAME]) == 1

    assert 'player_id' in error[Response.ERROR_DETAILS_FIELD_NAME]
    assert len(error[Response.ERROR_DETAILS_FIELD_NAME]['player_id']) == 1
    assert error[Response.ERROR_DETAILS_FIELD_NAME]['player_id'][0] == 'Missing data for ' \
                                                                       'required field.'

    players_count = await PlayerStatistic.collection.count_documents({})
    assert players_count == 0

    await PlayerStatistic.collection.delete_many({})


@pytest.mark.asyncio
async def test_worker_returns_validation_for_invalid_player_id(sanic_server):
    await PlayerStatistic.collection.delete_many({})

    client = RpcAmqpClient(
        sanic_server.app,
        routing_key=REQUEST_QUEUE,
        request_exchange=REQUEST_EXCHANGE,
        response_queue='',
        response_exchange=RESPONSE_EXCHANGE
    )
    response = await client.send(payload={'player_id': "INVALID_OBJECT_ID"})

    assert Response.ERROR_FIELD_NAME in response.keys()
    error = response[Response.ERROR_FIELD_NAME]

    assert Response.ERROR_TYPE_FIELD_NAME in error.keys()
    assert error[Response.ERROR_TYPE_FIELD_NAME] == VALIDATION_ERROR

    assert Response.ERROR_DETAILS_FIELD_NAME in error.keys()
    assert len(error[Response.ERROR_DETAILS_FIELD_NAME]) == 1

    assert 'player_id' in error[Response.ERROR_DETAILS_FIELD_NAME]
    assert len(error[Response.ERROR_DETAILS_FIELD_NAME]['player_id']) == 1
    assert error[Response.ERROR_DETAILS_FIELD_NAME]['player_id'][0] == 'Invalid ObjectId.'

    players_count = await PlayerStatistic.collection.count_documents({})
    assert players_count == 0

    await PlayerStatistic.collection.delete_many({})
