from copy import deepcopy

import pytest
from bson import ObjectId
from sage_utils.amqp.clients import RpcAmqpClient
from sage_utils.constants import VALIDATION_ERROR, NOT_FOUND_ERROR
from sage_utils.wrappers import Response

from app.statistics.documents import PlayerStatistic
from app.workers.update_player_statistics import UpdatePlayerStatisticsWorker


REQUEST_QUEUE = UpdatePlayerStatisticsWorker.QUEUE_NAME
REQUEST_EXCHANGE = UpdatePlayerStatisticsWorker.REQUEST_EXCHANGE_NAME
RESPONSE_EXCHANGE = UpdatePlayerStatisticsWorker.RESPONSE_EXCHANGE_NAME
PLAYER_NOT_FOUND_ERROR = UpdatePlayerStatisticsWorker.PLAYER_NOT_FOUND_ERROR


@pytest.mark.asyncio
async def test_worker_returns_updated_player_statistics(sanic_server):
    await PlayerStatistic.collection.delete_many({})

    player_id = str(ObjectId())
    create_data = {
        'player_id': player_id,
        'total_games': 10,
        'wins': 5,
        'loses': 5,
        'rating': 2500
    }
    object = PlayerStatistic(**create_data)
    await object.commit()

    players_count = await PlayerStatistic.collection.count_documents({})
    assert players_count == 1

    update_data = deepcopy(create_data)
    update_data.update({
        'total_games': 12,
        'wins': 6,
        'loses': 6,
        'rating': 2500
    })
    client = RpcAmqpClient(
        sanic_server.app,
        routing_key=REQUEST_QUEUE,
        request_exchange=REQUEST_EXCHANGE,
        response_queue='',
        response_exchange=RESPONSE_EXCHANGE
    )
    response = await client.send(payload=update_data)

    assert Response.EVENT_FIELD_NAME in response.keys()
    assert Response.CONTENT_FIELD_NAME in response.keys()
    content = response[Response.CONTENT_FIELD_NAME]

    assert len(list(content.keys())) == 6
    assert set(content.keys()) == {'id', 'player_id', 'total_games', 'wins', 'loses', 'rating'}

    assert content['player_id'] == update_data['player_id']
    assert content['total_games'] == update_data['total_games']
    assert content['wins'] == update_data['wins']
    assert content['loses'] == update_data['loses']
    assert content['rating'] == update_data['rating']

    await PlayerStatistic.collection.delete_many({})


@pytest.mark.asyncio
async def test_worker_returns_an_error_for_extra_fields_by_default(sanic_server):
    await PlayerStatistic.collection.delete_many({})

    player_id = str(ObjectId())
    create_data = {
        'player_id': player_id,
        'total_games': 10,
        'wins': 5,
        'loses': 5,
        'rating': 2500
    }
    object = PlayerStatistic(**create_data)
    await object.commit()

    players_count = await PlayerStatistic.collection.count_documents({})
    assert players_count == 1

    update_data = deepcopy(create_data)
    update_data.update({
        'total_games': 12,
        'wins': 6,
        'loses': 6,
        'rating': 2500,
        'winrate': 50,
        'nickname': 'user'
    })
    client = RpcAmqpClient(
        sanic_server.app,
        routing_key=REQUEST_QUEUE,
        request_exchange=REQUEST_EXCHANGE,
        response_queue='',
        response_exchange=RESPONSE_EXCHANGE
    )
    response = await client.send(payload=update_data)

    assert Response.EVENT_FIELD_NAME in response.keys()
    assert Response.ERROR_FIELD_NAME in response.keys()
    error = response[Response.ERROR_FIELD_NAME]

    assert len(list(error.keys())) == 2
    assert set(error.keys()) == {'type', 'details'}

    assert error['type'] == VALIDATION_ERROR

    await PlayerStatistic.collection.delete_many({})


@pytest.mark.asyncio
async def test_worker_returns_no_changed_player_statistics(sanic_server):
    await PlayerStatistic.collection.delete_many({})

    player_id = str(ObjectId())
    create_data = {
        'player_id': player_id,
        'total_games': 10,
        'wins': 5,
        'loses': 5,
        'rating': 2500
    }
    object = PlayerStatistic(**create_data)
    await object.commit()

    players_count = await PlayerStatistic.collection.count_documents({})
    assert players_count == 1

    update_data = deepcopy(create_data)
    client = RpcAmqpClient(
        sanic_server.app,
        routing_key=REQUEST_QUEUE,
        request_exchange=REQUEST_EXCHANGE,
        response_queue='',
        response_exchange=RESPONSE_EXCHANGE
    )
    response = await client.send(payload=update_data)

    assert Response.EVENT_FIELD_NAME in response.keys()
    assert Response.CONTENT_FIELD_NAME in response.keys()
    content = response[Response.CONTENT_FIELD_NAME]

    assert len(list(content.keys())) == 6
    assert set(content.keys()) == {'id', 'player_id', 'total_games', 'wins', 'loses', 'rating'}

    assert content['player_id'] == update_data['player_id']
    assert content['total_games'] == update_data['total_games']
    assert content['wins'] == update_data['wins']
    assert content['loses'] == update_data['loses']
    assert content['rating'] == update_data['rating']

    await PlayerStatistic.collection.delete_many({})
