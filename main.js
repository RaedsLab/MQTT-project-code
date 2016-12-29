const elasticsearch = require('elasticsearch');
const mqtt = require('mqtt');
const chalk = require('chalk');
const jsonSize = require('json-size');
const ProgressBar = require('ascii-progress');
const memwatch = require('memwatch-next');

const INDEX = 'mqtt',
    TYPE = 'eclipse',
    BUFFER_SIZE = 300; // kilobytes

const bar = new ProgressBar({
    schema: ':bar.green [:current.bold.yellow/:total] Kilobytes (:percent) :elapseds.yellow',
    total: BUFFER_SIZE,
    colors: ':bar.red :percent.green'
});


let bulkRecords = [],
    tempRecords = [],
    mutex = false,
    heap = null;

const ESclient = new elasticsearch.Client({
    host: 'localhost:9200',
    log: 'error'
});

const client = mqtt.connect('mqtt://iot.eclipse.org');

client.on('connect', () => {
    client.subscribe('#');
});

client.on('message', (topic, message) => {
    addARecord(message.toString(), topic);
});


function addARecord(message, topic) {

    const record = {
        date: new Date().toISOString(),
        message: message,
        topic: topic
    };

    if (mutex) {
        tempRecords.push({create: {_index: INDEX, _type: TYPE}});
        tempRecords.push(record);
    } else {

        if (tempRecords.length > 0) {
            console.log(`Temporary : ${chalk.yellow(tempRecords.length)} Size : ${jsonSize(tempRecords) / 1024} Kilobytes`);
            bulkRecords = tempRecords;
            tempRecords = [];
            console.log("Temporary => data");
        }

        bulkRecords.push({create: {_index: INDEX, _type: TYPE}});
        bulkRecords.push(record);

        const bulkSize = jsonSize(bulkRecords) / 1024;

        if (bulkSize >= BUFFER_SIZE) {
            if (heap == null) {
                heap = process.memoryUsage().heapUsed;
            } else {
                const heapDiff = process.memoryUsage().heapUsed - heap;
                heap = process.memoryUsage().heapUsed;
                console.log(`Heap : ${((heapDiff / 1024) / 1024).toFixed(2)}`)
            }

            console.log('');
            console.log(`Total size : ${chalk.bold.underline(bulkSize.toFixed(2))} Kilobytes`);
            console.log(`Number of elements ${bulkRecords.length}`);
            saveBulk();
        } else {
            bar.update(bulkSize / BUFFER_SIZE);
        }
    }
}

function saveBulk() {
    mutex = true;
    ESclient.bulk({
        body: bulkRecords
    }, function (err, resp) {
        bulkRecords = []; // Reset the array
        mutex = false; // Release the mutex

        if (err) {
            console.log(chalk.bold.red(`${err}`));
        } else {
            console.log(chalk.bold.green('All is good'));
            if (resp.errors) {
                console.log(chalk.bold.red('WITH ERRORS'));
            }
        }
    });
}

memwatch.on('leak', function (info) {
    console.error('Memory leak detected: ', info);
    //console.log(JSON.stringify(process.memoryUsage()))
});

/*
 Output sample:
 ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇———————— [260/300] Kilobytes (87%) 22.2s
 Total size : 443.67 Kilobytes
 Number of elements 41764
 All is good
 TEMP : 44642
 Passed from temp to data
 */

