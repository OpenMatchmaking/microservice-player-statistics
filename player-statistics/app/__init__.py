from sanic import Sanic
from sanic.response import text
from sanic_mongodb_ext import MongoDbExtension
from sanic_amqp_ext import AmqpExtension

from app.workers import InitPlayerStatisticsWorker, RetrievePlayerStatisticsWorker, \
    UpdatePlayerStatisticsWorker


app = Sanic('microservice-player-statistics')
app.config.from_envvar('APP_CONFIG_PATH')


# Extensions
AmqpExtension(app)
MongoDbExtension(app)

# RabbitMQ workers
app.amqp.register_worker(InitPlayerStatisticsWorker(app))
app.amqp.register_worker(RetrievePlayerStatisticsWorker(app))
app.amqp.register_worker(UpdatePlayerStatisticsWorker(app))


# Public API
async def health_check(request):
    return text('OK')


app.add_route(health_check, '/player-statistics/api/health-check',
              methods=['GET', ], name='health-check')
