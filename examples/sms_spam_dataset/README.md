# SMS Spam Dataset
This example uses three different machine learning models to classify a 
collection of text messages as either legitimate or spam.
`sms_spam_detector_serial.py` is a serial
implementation that trains all three one after another. 
`sms_spam_detector_parallel.py` trains all three in parallel using a 
pipeline.

The data was provided courtesy of Tiago A. Almeida and José María Gómez Hidalgo
and can be found [here](http://www.dt.fee.unicamp.br/~tiago/smsspamcollection/).

## Setup
You will need to install the needed Python libraries with
`pip3 install -r requirements.txt`.
