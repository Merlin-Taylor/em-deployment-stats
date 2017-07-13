'use strict'

let AWS = require('aws-sdk');
let fp = require('lodash/fp');

// let params = {
//     TableName: 'ConfigCompletedDeployments',
//     Limit: 50,
//     ProjectionExpression: '#StartDate, #Value.#OwningCluster, #Value.#Status',
//     FilterExpression: '#Value.#EnvironmentType IN (:prod, :management)',
//     ExpressionAttributeNames: {
//         '#EnvironmentType': 'EnvironmentType',
//         '#OwningCluster': 'OwningCluster',
//         '#StartDate': 'StartDate',
//         '#Value': 'Value',
//         '#Status': 'Status',
//     },
//     ExpressionAttributeValues: {
//         ':management': 'Management',
//         ':prod': 'Prod',
//     }
// };

let params = {
    TableName: 'ConfigCompletedDeployments',
    Limit: 50,
    ProjectionExpression: '#StartDate, #Value.#EnvironmentType, #Value.#ServiceName, #Value.#Status',
    FilterExpression: 'contains(#Value.#ExecutionLog, :substring)',
    ExpressionAttributeNames: {
        '#EnvironmentType': 'EnvironmentType',
        '#ExecutionLog': 'ExecutionLog',
        '#ServiceName': 'ServiceName',
        '#StartDate': 'StartDate',
        '#Value': 'Value',
        '#Status': 'Status',
    },
    ExpressionAttributeValues: {
        ':substring': 'Appended appspec.yml',
    },
};
let dynamo = new AWS.DynamoDB.DocumentClient({ region: 'eu-west-1' });
let acc = [];
let promise = new Promise((resolve, reject) => {
    dynamo.scan(params).eachItem((e, x) => {
        if (e) {
            console.error(e);
            reject(e);
        } else if (x === null) {
            resolve(acc);
        } else {
            acc.push({
                StartDate: x.StartDate,
                EnvironmentType: fp.get(['Value', 'EnvironmentType'])(x),
                Status: fp.get(['Value', 'Status'])(x),
                ServiceName: fp.get(['Value', 'ServiceName'])(x),
            });
        }
    });
});

// let results = fp.flow(fp.groupBy(({ EnvironmentType, ServiceName, StartDate, Status }) => [EnvironmentType, StartDate, Status]),
//     fp.toPairs,
//     fp.map(([key, items]) => [fp.head(items).EnvironmentType, fp.head(items).StartDate, fp.head(items).Status, items.length]),
//     fp.sortBy(x => x[0])
// )(acc)
results = acc;

var ws = fs.createWriteStream('./results.csv', { autoClose: true });
ws.on('error', error => console.log(error));
results.map(([EnvironmentType, StartDate, success, count]) => `${EnvironmentType};${StartDate};${success};${count}
`).forEach(str => ws.write(str));
ws.end();
