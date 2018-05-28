from sanic import Sanic
from sanic.response import text
from sanic_mongodb_ext import MongoDbExtension
from sanic_amqp_ext import AmqpExtension


app = Sanic('microservice-auth')
app.config.from_envvar('APP_CONFIG_PATH')


# Extensions
AmqpExtension(app)
MongoDbExtension(app)

# RabbitMQ workers


# Public API
async def health_check(request):
    return text('OK')


app.add_route(health_check, '/player-statistics/api/health-check',
              methods=['GET', ], name='health-check')
