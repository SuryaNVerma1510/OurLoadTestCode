
const { 
    v1: uuidv1,
    v4: uuidv4,
  } = require('uuid');

const express = require('express');
var kafka = require('kafka-node'),
    Promise = require('promise'),
    Producer = kafka.Producer,
    Consumer = kafka.Consumer,
    KeyedMessage = kafka.KeyedMessage,    
    client = new kafka.KafkaClient({kafkaHost : '10.16.4.5:9092'}),
    producer = new Producer(client),
    km = new KeyedMessage('key', 'message'),
    consumedTopics = [],
    consumer = null,
    Offset = kafka.Offset;
    promiseResolvers = {};
    promises = [];

//     // key : uuid.v6(),
producer.on('ready', function () {
    // var payloads = [
    //     { key : uuidv4(),topic: 'dev_PerformanceTest', messages: 'hi', partition: 0 }
    // ];
    // produce(payloads);

            var options = {
                groupId: 'kafka-node-group-dev_PerformanceTest_Response',
                autoCommit: true
                // autoCommitIntervalMs: 5000,
                // fetchMaxWaitMs: 100,
                // fetchMinBytes: 1,
                // fetchMaxBytes: 1024 * 1024,
                 ,fromOffset: true
                // encoding: 'utf8',
                // keyEncoding: 'utf8'
            };
            consumer = new Consumer(
                client,
                [
                    {topic: 'dev_PerformanceTest_Response', partition: 0}
                ],
                options
            );
            var offset = new Offset(client);
            consumer.on('message', function (message) {       
                //console.log('consumer');
                //console.log(message);

                if(promiseResolvers[message.key] != null && promiseResolvers[message.key] != undefined)
                {
                    promiseResolvers[message.key](message.value);
                    promiseResolvers[message.key] = null;
                }
            });

            consumer.on('error', function (err) {
                console.log('error', err);
              });

              consumer.on('offsetOutOfRange', function (topic) {
                topic.maxNum = 2;
                offset.fetch([topic], function (err, offsets) {
                    if (err) {
                    return console.error(err);
                    }
                    var min = Math.min(offsets[topic.topic][topic.partition]);
                    consumer.setOffset(topic.topic, topic.partition, min);
                });
              });
});

function consume(topic)
{
    if(consumedTopics.indexOf(topic) == -1)
    {
        consumedTopics.push(topic);        
        consumer.addTopics(consumedTopics, function (err, added) {
        });
    }
}

function produce(payloads)
{
    producer.send(payloads, function (err, data) {
        
    });
}

function createTopic(topicName)
{
    client.createTopics(topicName, (error, result) => {
        // result is an array of any errors if a given topic could not be created
      });
}


// Express Initialize
const app = express();
const port = 8501;
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.listen(port,()=> {
console.log('listen port 8501');
})

app.post('/home', (req,res)=>{
    var topic = req.body.topic;
    var responseTopic = req.body.payload.responseTopic;
    req.body.payload.key = uuidv4();
    consume(responseTopic);

    var promiseResolve = null;
    var customPromise = new Promise((resolve, reject) => {
        promiseResolve = resolve;
        setTimeout(() => {
            resolve('timeout occured');
          }, 10000);
      });
    
      promiseResolvers[req.body.payload.key] = promiseResolve;
      promises.push(customPromise);
    var payloads = [
        { key : req.body.payload.key,topic: topic, messages: JSON.stringify(req.body.payload)}
    ];
    produce(payloads);
    // var apiresponse = '';    
    customPromise.then(response => {   
    //Promise.all(promises).then((values) => {
        //console.log('then');     
        //console.log(response);  
        try {
            var responsejson = JSON.parse(response);
            res.send(responsejson);
        }
        catch {
            res.send(response);
        }  
      })
      .catch(err => {
        console.log('err');     
        console.log(err);
        res.send(err);
      });
    //   console.log('response');
    //   console.log(apiresponse);

    

    
    })