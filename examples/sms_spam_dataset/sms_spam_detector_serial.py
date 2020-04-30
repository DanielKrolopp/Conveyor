from nltk.util import ngrams  # function for making ngrams
import pandas as pd
from collections import defaultdict

from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, precision_recall_fscore_support
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier


def extract_ngrams_from_line(string):
    splits = string.split()
    grams = ngrams(splits[1:], 1)
    gram_counts = defaultdict(int)
    for e in grams:
        gram_counts[str(e[0])] += 1
    gram_counts['classification'] = splits[0]

    return gram_counts


def evaluate(model, model_name, val_X, val_y, labels):
    y_predictions = model.predict(val_X)

    cmtx = pd.DataFrame(
        confusion_matrix(val_y, y_predictions, labels=labels),
        index=['actual: ' + label for label in labels],
        columns=['predicted: ' + label for label in labels]
    )
    precision, recall, fbeta, _ = precision_recall_fscore_support(
        val_y, y_predictions, average='weighted')

    print('\n\n----------', model_name, '----------\n')
    print('\n', cmtx, '\n\n')
    print('precision:', precision)
    print('recall:', recall)
    print('fbeta score:', fbeta)


def main():
    with open("sms_data.txt", "r", encoding='latin-1') as file:
        text = file.read().split('\n')
        data = []
        for line in text:
            if len(line) == 0:  # eof
                break
            grams = extract_ngrams_from_line(line)
            data.append(grams)
        df = pd.DataFrame(data)

        models = [DecisionTreeClassifier(), RandomForestClassifier(), KNeighborsClassifier()]
        model_names = ['Decision tree', 'Random forest', 'K neighbors']

        df = df.fillna(0)
        text = None

        X = df.drop(columns=['classification'])
        y = df.classification

        train_X, val_X, train_y, val_y = train_test_split(X, y, random_state=0)

        for model, name in zip(models, model_names):
            model.fit(train_X, train_y)
            labels = list(set(train_y))
            evaluate(model, name, val_X, val_y, labels)


if __name__ == "__main__":
    main()
