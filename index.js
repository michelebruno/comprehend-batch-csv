const AWS = require('aws-sdk');
const csv = require('csv-parser');
const fs = require('fs');

if (!fs.existsSync('./results/')) {
  fs.mkdirSync('./results/');
}


const d = new Date();

const path = 'results/comprehend_' + d.getFullYear() + d.getMonth() + d.getDate() +
    d.getHours() + d.getMinutes() +( d.getSeconds() < 10 ? '0' +  d.getSeconds() : d.getSeconds() )+ '.csv';

if (!fs.existsSync(path)) {
  fs.writeFileSync(path,  'General,Positive,Negative,Neutral,Mixed,Text\n')
}


const writer = require('csv-writer').createObjectCsvWriter({
  path, header: [
    {id: 'General', title: 'General'},
    {id: 'Positive', title: 'Positive'},
    {id: 'Negative', title: 'Negative'},
    {id: 'Neutral', title: 'Neutral'},
    {id: 'Mixed', title: 'Mixed'},
    {id: 'Text', title: 'Text'},
  ],
  append: true
});

AWS.config.update({region: 'eu-west-2'});

AWS.config.getCredentials(function(err) {
  if (err) console.log(err.stack);
  else {
    console.log('Got access key ' + AWS.config.credentials.accessKeyId);
  }
});

const comprehend = new AWS.Comprehend();

const results = [];

let temp;
let i = 0;

fs.createReadStream('opinioni.csv').
    pipe(csv()).
    on('data', (data) => {
      results.push(data.Opinione);
    }).
    on('end', () => {
      console.log('Risultati: ' + results.length);

      console.log('lancio comprehend');

      batch()
    });

function batch() {

  if (i > results.length) {
    return;
  }
  console.log('Batching from ' + i + ' to ' + (i + 24));

  temp = results.slice(i, i + 24);
  comprehend.batchDetectSentiment({
    LanguageCode: 'it',
    TextList: temp,
  }, (err, res) => processData(err, res, i));

  i += 25;

}

async function processData(err, data, i) {
  let scores = [];
  console.log(i);
  if (err) {
    console.log(err, err.stack);
  } else {
    data.ResultList.forEach(res => {
      scores.push({
        Text: temp[res.Index],
        General: res.Sentiment,
        Positive: res.SentimentScore.Positive.toFixed(4),
        Negative: res.SentimentScore.Negative.toFixed(4),
        Neutral: res.SentimentScore.Neutral.toFixed(4),
        Mixed: res.SentimentScore.Mixed.toFixed(4),
      });
    });

    if (data.ErrorList?.length) console.log(data.ErrorList);

    await writer.writeRecords(scores).then(batch);

  }
}