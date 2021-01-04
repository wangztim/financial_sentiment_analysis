from transformers import (DistilBertForSequenceClassification,
                          DistilBertTokenizerFast, DistilBertConfig)
from classes.message import Sentiment
from typing import Union, List
from torch.nn.functional import softmax


class SentimentAnalyzer:
    model: DistilBertForSequenceClassification
    tokenizer: DistilBertTokenizerFast

    max_token_length = 160
    uncertainty_threshhold = 0.05

    def __init__(self, path=None, model_name=None):
        if path:
            self.model = DistilBertForSequenceClassification.from_pretrained(
                path)
            self.tokenizer = DistilBertTokenizerFast.from_pretrained(path +
                                                                     "/model")
        elif model_name:
            config = DistilBertConfig.from_pretrained(model_name,
                                                      return_dict=True,
                                                      num_labels=2)
            self.model = DistilBertForSequenceClassification.from_pretrained(
                model_name, config=config)
            self.tokenizer = DistilBertTokenizerFast.from_pretrained(
                model_name)

    def classify(self, input_str: str) -> Union[Sentiment, List[Sentiment]]:
        tokens = self.tokenize(input_str)
        logits = self.model(**tokens).logits
        softmaxed_list = softmax(logits, dim=1).tolist()
        outputs = [self._sentiment_mapper(e) for e in softmaxed_list]
        if len(outputs) == 1:
            return outputs[0]
        else:
            return outputs

    def tokenize(self, input_str: Union[str, List[str]]) -> dict:
        return self.tokenizer(input_str,
                              max_length=SentimentAnalyzer.max_token_length,
                              padding="max_length",
                              return_tensors='pt',
                              truncation=True)

    def _sentiment_mapper(self, scores) -> Sentiment:
        if abs(scores[0] - scores[1]) < self.uncertainty_threshhold:
            return Sentiment.UNCERTAIN

        if scores[0] > scores[1]:
            return Sentiment(0)
        else:
            return Sentiment(1)
