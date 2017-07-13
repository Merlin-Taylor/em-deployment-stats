'use strict'

let AWS = require('aws-sdk');
let fs = require('fs');
let fp = require('lodash/fp');

function executeQueryAsync(dynamo, params) {
    let acc = [];
    let promise = new Promise((resolve, reject) => {
        dynamo.scan(params).eachItem((e, x) => {
            if (e) {
                console.error(e);
                reject(e);
            } else if (x === null) {
                resolve(acc);
            } else {
                process.stderr.write('.');
                acc.push({
                    StartDate: x.StartDate,
                    EnvironmentType: fp.get(['Value', 'EnvironmentType'])(x),
                    Status: fp.get(['Value', 'Status'])(x),
                    ServiceName: fp.get(['Value', 'ServiceName'])(x),
                });
            }
        });
    });
    return promise.then(acc);
}

let postprocess = fp.flow(fp.groupBy(({ EnvironmentType, ServiceName, StartDate, Status }) => [EnvironmentType, StartDate, Status]),
    fp.toPairs,
    fp.map(([key, items]) => [fp.head(items).EnvironmentType, fp.head(items).StartDate, fp.head(items).Status, items.length]),
    fp.sortBy(x => x[0])
);

function writeResultsTo(file) {
    return (results) => {
        var ws = fs.createWriteStream(file, { autoClose: true });
        ws.on('error', error => console.log(error));
        results.map(([EnvironmentType, StartDate, success, count]) => `${EnvironmentType},${StartDate},${success},${count}\n`).forEach(str => ws.write(str));
        ws.end();
    }
}

let params = {
    TableName: 'ConfigCompletedDeployments',
    Limit: 50,
    ProjectionExpression: '#StartDate, #Value.#EnvironmentType, #Value.#ServiceName, #Value.#Status',
    ExpressionAttributeNames: {
        '#EnvironmentType': 'EnvironmentType',
        '#ServiceName': 'ServiceName',
        '#StartDate': 'StartDate',
        '#Value': 'Value',
        '#Status': 'Status',
    }
};

let dynamo = new AWS.DynamoDB.DocumentClient({ region: 'eu-west-1' });

executeQueryAsync(dynamo, params)
    .then(postprocess)
    .then(writeResultsTo('./results.csv'));
