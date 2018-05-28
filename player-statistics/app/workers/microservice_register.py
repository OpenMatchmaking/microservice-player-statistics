from sage_utils.amqp.workers import BaseRegisterWorker
from sage_utils.amqp.mixins import SanicAmqpExtensionMixin


class MicroserviceRegisterWorker(SanicAmqpExtensionMixin, BaseRegisterWorker):

    def get_microservice_data(self, _app):
        return {
            'name': self.app.config['SERVICE_NAME'],
            'version': self.app.config['SERVICE_VERSION'],
            'permissions': [
                {
                    'codename': 'player-stats.statistic.init',
                    'description': 'Initializes statistics from an empty state for the new player',
                },
                {
                    'codename': 'player-stats.statistic.retrieve',
                    'description': 'Get a player statistics',
                },
                {
                    'codename': 'player-stats.statistic.update',
                    'description': 'Update a player statistics',
                },
            ]
        }
