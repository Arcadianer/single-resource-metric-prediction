from gluonts.model.predictor import Predictor
import json
from log import log
from pandas import DataFrame
from . import data
import pandas as pd
import matplotlib.pyplot as plt
from gluonts.model.deepar import DeepAREstimator
from gluonts.mx.trainer import Trainer
from gluonts.dataset.common import ListDataset
from gluonts.evaluation.backtest import make_evaluation_predictions
from gluonts.evaluation import Evaluator
from gluonts.distribution import NegativeBinomialOutput, PoissonOutput

model_fit: Predictor=0

def fitDeepARNegativeBinomial(length: int,context: int,epochs: int):
    global model_fit
    reportDict=({'model_used':"deepar",'forecast-length':length,'context':context,'epochs':epochs,'dist':"NegativeBinomial"})
    log.log_data("model",reportDict)
    trainingset= ListDataset(
    [{"start": data.trainingset.index[0], "target": data.trainingset.values.flatten()}],
    freq = "D"
    )

    estimator = DeepAREstimator(freq="D", prediction_length=length,context_length=context,distr_output = NegativeBinomialOutput(),scaling=True, trainer=Trainer(epochs=epochs))
    
    model_fit = estimator.train(training_data=trainingset)
 

def fitDeepARStudentT(length: int,context: int,epochs: int):
    global model_fit
    trainingset= ListDataset(
    [{"start": data.trainingset.index[0], "target": data.trainingset.values.flatten()}],
    freq = "D"
    )

    estimator = DeepAREstimator(freq="D", prediction_length=length,context_length=context,scaling=True, trainer=Trainer(epochs=epochs))
    
    model_fit = estimator.train(training_data=trainingset)
def evaluate():
    testset= ListDataset(
        [{"start": data.trainingset.index[0], "target": data.trainingset.values.flatten()}],
        freq = "1D"
    )
    
    tts=data.trainingset.append(data.testset[:model_fit.prediction_length],verify_integrity=True).resample("D").sum()
    a=model_fit.predict(testset)
    raw_forecast=list(a)
    forecasts = raw_forecast[0]
    #tss = list(ts_it)[0]
    eval = Evaluator(quantiles=[0.1, 0.5, 0.9])
    pea=eval.get_metrics_per_ts(tts,forecasts)
    metrics=json.dumps(pea, indent=4)
    plot_prob_forecasts(tts,forecasts)
    return pea

def plot_prob_forecasts(ts_entry, forecast_entry):
    plot_length = 150
    prediction_intervals = (50.0, 90.0)
    legend = ["observations", "median prediction"] + [f"{k}% prediction interval" for k in prediction_intervals][::-1]

    fig, ax = plt.subplots(1, 1, figsize=(10, 7))
    ts_entry[-plot_length:].plot(ax=ax)  # plot the time series
    forecast_entry.plot(prediction_intervals=prediction_intervals, color='g')
    plt.grid(which="both")
    plt.legend(legend, loc="upper left")


def forecast(target: DataFrame):
    testset= ListDataset(
        [{"start": target.index[0], "target": target.values.flatten()}],
        freq = "1D"
    )
    forecast=list(model_fit.predict(testset))[0]
    samples = forecast.mean
    h = samples.shape[0]
    dates = pd.date_range(forecast.start_date, freq=forecast.freq, periods=h)
    return pd.DataFrame(samples.T, index=dates)
