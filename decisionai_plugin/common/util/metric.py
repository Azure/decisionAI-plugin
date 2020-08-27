import tensorflow as tf
import json
import logging

from .meta import update_state

logger = logging.getLogger(__name__)

class Metric:
    def __init__(self, epochs, epoch_th, loss, valid_loss):
        self.__epochs = epochs
        self.__current_epoch = epoch_th
        self.__loss = loss
        self.__valid_loss = valid_loss

    @property
    def epochs(self):
        return self.__epochs

    @property
    def epoch(self):
        return self.__current_epoch

    @property
    def loss(self):
        return self.__loss

    @property
    def valid_loss(self):
        return self.__valid_loss


class MetricSender:
    def __init__(self, config, subscription, model_id):
        self.__config = config
        self.__subscription = subscription
        self.__model_id = model_id
        pass

    def send(self, metric: Metric):
        txt = 'epoch: ' + str(metric.epoch) + '/' + str(metric.epochs) + ', loss: ' + str(metric.loss) + ', validate loss: ' + str(metric.valid_loss)
        update_state(self.__config, self.__subscription, self.__model_id, None, None, txt)
        info = {'epochs': metric.epochs, 'epoch': metric.epoch, 'loss': metric.loss, 'val_loss': metric.valid_loss}
        # logger.info("Current metric : {0}".format(json.dumps(info)))

class MetricCollector(tf.keras.callbacks.Callback):
    def __init__(self, epochs, metric_sender: MetricSender):
        super().__init__()
        self.__epochs = epochs
        self.__sender = metric_sender

    def on_epoch_end(self, epoch, logs=None):
        metric = Metric(epochs=self.__epochs, epoch_th=epoch, loss=logs['loss'],
                        valid_loss=logs['val_loss'] if 'val_loss' in logs else None)
        self.__sender.send(metric)
