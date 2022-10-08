const AWS = require("aws-sdk");
const express = require("express");
const serverless = require("serverless-http");

const app = express();

const TASKS_TABLE = process.env.TASKS_TABLE;
const WORKERS_TABLE = process.env.WORKERS_TABLE;
const QUEUES_TABLE = process.env.QUEUES_TABLE;
const WORKSPACES_TABLE = process.env.WORKSPACES_TABLE;
const RESERVATIONS_TABLE = process.env.RESERVATIONS_TABLE;
const RESERVATIONS_TIMEOUT_TABLE = process.env.RESERVATIONS_TIMEOUT_TABLE;
const ACTIVITIES_TABLE = process.env.ACTIVITIES_TABLE;
const EVENTS_TABLE = process.env.EVENTS_TABLE;
const WORKFLOWS_TABLE = process.env.WORKFLOWS_TABLE;
const WORKER_ATTRIBUTES_TABLE = process.env.WORKER_ATTRIBUTES_TABLE;
const WORKER_QUEUE_TABLE = process.env.WORKER_QUEUE_TABLE;
const dynamoDbClient = new AWS.DynamoDB.DocumentClient();
const dynamoDb = new AWS.DynamoDB();
const { v4: uuidv4 } = require('uuid');

app.use(express.json());

app.post("/workers", async function (req, res) {
  const { attributes, activityId, workspaceId } = req.body;

  if (typeof attributes !== "object") {
    res.status(400).json({ error: '"attributes" must be a object' });
  } else if (typeof workspaceId !== "string") {
    res.status(400).json({ error: '"workspaceId" must be a string' });
  } else if (typeof activityId !== "string") {
    res.status(400).json({ error: '"activityId" must be a string' });
  }

  const id = uuidv4();

  try {
    let params = {
      TableName: ACTIVITIES_TABLE,
      Key: {
        id: activityId,
      },
    };
    const { Item } = await dynamoDbClient.get(params).promise();

    if (Item) {
      const { isAvailable } = Item;
      const id = uuidv4();

      params = {
        TableName: WORKERS_TABLE,
        Item: {
          id,
          workspaceId,
          isAvailable,
          activityId,
          dateCreated: (new Date()).toUTCString(),
          attributes,
        },
      };

      await dynamoDbClient.put(params).promise();

      for (let attributeName in attributes) {
        if (attributes.hasOwnProperty(attributeName)) {
          const attributeValue = attributes[attributeName];

          params = {
            TableName: WORKER_ATTRIBUTES_TABLE,
            Item: {
              id: uuidv4(),
              attributeName,
              attributeValue,
              workerId: id,
              workspaceId,
	    }
	  };

          await dynamoDbClient.put(params).promise();

          params = {
            ExpressionAttributeValues: {
              ":workspaceId": {
                S: workspaceId
              },
              ":attributeName": {
                S: attributeName
              },
              ":attributeValue": {
                S: attributeValue
              }
            },
            KeyConditionExpression: 'workspaceId = :workspaceId',
            FilterExpression: 'attributeName = :attributeName and attributeValue = :attributeValue',
            ProjectionExpression: 'id',
            TableName: QUEUES_TABLE,
          };

          const queues = await dynamoDb.query(params).promise();

          if (queues.Items.length) {
            for (let queue of queues.Items) {
	      console.log(queue);
              params = {
                TableName: WORKER_QUEUE_TABLE,
                Key: {
                  queueId: queue.id.S,
                  workerId: id
                },
              };

              await dynamoDbClient.delete(params).promise();

              params = {
                TableName: WORKER_QUEUE_TABLE,
                Item: {
                  id: uuidv4(),
                  workerId: id,
                  queueId: queue.id.S,
                  workspaceId,
                }
              };
              await dynamoDbClient.put(params).promise();
            }
          }
        }
      }

      res.json({ id, activityId, workspaceId, attributes });
    } else {
      res.status(400).json({ error: 'Activity not found' });
    }
    res.status(200)
  } catch (error) {
    console.log(error);
    res.status(500).json({ error: "Could not create worker" });
  }
});

app.patch("/workers", async function (req, res) {
  const { id, attributes, activityId, workspaceId } = req.body;

  if (typeof attributes !== "object") {
    res.status(400).json({ error: '"attributes" must be a object' });
  } else if (typeof id !== "string") {
    res.status(400).json({ error: '"id" must be a string' });
  } else if (typeof activityId !== "string") {
    res.status(400).json({ error: '"activityId" must be a string' });
  } else if (typeof workspaceId !== "string") {
    res.status(400).json({ error: '"workspaceId" must be a string' });
  }

  try {
    let params = {
      TableName: ACTIVITIES_TABLE,
      Key: {
        id: activityId,
      },
    };
    const { Item } = await dynamoDbClient.get(params).promise();

    if (!Item) {
      return res.status(400).json({ error: 'Activity not found' });
    }

    const { isAvailable } = Item;

    params = {
      TableName: WORKERS_TABLE,
      ExpressionAttributeValues: {
        ':attributes': attributes.skills,
        ':activityId': activityId,
      },
      Key: {
        workspaceId,
        id,
      },
      UpdateExpression: 'set attributes.skills = :attributes, activityId = :activityId',
    };
    await dynamoDbClient.update(params).promise();

    params = {
      TableName: WORKER_ATTRIBUTES_TABLE,
      Key: {
        workspaceId,
        workerId: id,
      },
    };
    await dynamoDbClient.delete(params).promise();

    for (let attributeName in attributes) {
      if (attributes.hasOwnProperty(attributeName)) {
        const attributeValue = attributes[attributeName];

        params = {
          TableName: WORKER_ATTRIBUTES_TABLE,
          Item: {
            id: uuidv4(),
            attributeName,
            attributeValue,
            workerId: id,
            workspaceId,
          }
        };

        await dynamoDbClient.put(params).promise();

        params = {
          ExpressionAttributeValues: {
            ":workspaceId": {
              S: workspaceId
            },
            ":attributeName": {
              S: attributeName
            },
            ":attributeValue": {
              S: attributeValue
            }
          },
          KeyConditionExpression: 'workspaceId = :workspaceId',
          FilterExpression: 'attributeName = :attributeName and attributeValue = :attributeValue',
          ProjectionExpression: 'id',
          TableName: QUEUES_TABLE,
        };

        const queues = await dynamoDb.query(params).promise();

        if (queues.Items.length) {
          for (let queue of queues.Items) {
            params = {
              TableName: WORKER_QUEUE_TABLE,
              Key: {
                queueId: queue.id.S,
                workerId: id
              },
            };

            await dynamoDbClient.delete(params).promise();

            params = {
              TableName: WORKER_QUEUE_TABLE,
              Item: {
                id: uuidv4(),
                workerId: id,
                queueId: queue.id.S,
                workspaceId,
              }
            };
            await dynamoDbClient.put(params).promise();
          }
        }
      }
    }

    res.json({ id, activityId, workspaceId, attributes });
  } catch (error) {
    console.log(error);
    res.status(500).json({ error: "Could not update worker" });
  }
});

app.post("/activities", async function (req, res) {
  const { name, isAvailable, workspaceId } = req.body;

  if (typeof name !== "string") {
    res.status(400).json({ error: '"name" must be a string' });
  } else if (typeof isAvailable !== "string") {
    res.status(400).json({ error: '"isAvailable" must be a string' });
  } else if (typeof workspaceId !== "string") {
    res.status(400).json({ error: '"workspaceId" must be a string' });
  }

  const id = uuidv4();

  const params = {
    TableName: ACTIVITIES_TABLE,
    Item: {
      id,
      workspaceId,
      isAvailable,
      dateCreated: (new Date()).toUTCString(),
      name,
    },
  };

  try {
    await dynamoDbClient.put(params).promise();
    res.json({ id, name, workspaceId, isAvailable });
  } catch (error) {
    console.log(error);
    res.status(500).json({ error: "Could not create activity" });
  }
});

app.post("/tasks", async function (req, res) {
  const { workspaceId, workflowId, attributes } = req.body;

  if (typeof workspaceId !== "string") {
    res.status(400).json({ error: '"workspaceId" must be a string' });
  } else if (typeof workflowId !== "string") {
    res.status(400).json({ error: '"workflowId" must be a string' });
  } else if (typeof attributes !== "object") {
    res.status(400).json({ error: '"attributes" must be a object' });
  }

  let params = {
    TableName: WORKFLOWS_TABLE,
    Key: {
      id: workflowId,
      workspaceId: workspaceId,
    },
  };

  try {
    const { Item } = await dynamoDbClient.get(params).promise();

    if (!Item) {
      return res.status(400).json({ error: 'Workflow not found' });
    }

    const { filters } = Item;
    let queueId;

    filterBlock:
    for (let filter of filters) {
      const search = filter.condition.split("==");
      const property = search[0].trim();
      const value = search[1].trim();

      for (let key in attributes) {
        if (attributes.hasOwnProperty(key)) {
          if (key == property && value == attributes[key]) {
            queueId = filter.queueId;
            break filterBlock;
	  }
        }
      }
    }

    if (!queueId) {
      return res.status(400).json({ error: 'No matching queue found' });
    }

    const id = uuidv4();
    const currentStatus = "pending";

    params = {
      TableName: TASKS_TABLE,
      Item: {
        id,
        workspaceId,
        workflowId,
        currentStatus,
        dateCreated: (new Date()).toUTCString(),
        queueId,
        attributes,
      },
    };

    await dynamoDbClient.put(params).promise();
    res.json({ id, workspaceId, workflowId, currentStatus, queueId, attributes });
  } catch (error) {
    console.log(error);
    res.status(500).json({ error: "Could not create task" });
  }
});

app.post("/workflows", async function (req, res) {
  const { workspaceId, filters } = req.body;

  if (typeof workspaceId !== "string") {
    res.status(400).json({ error: '"workspaceId" must be a string' });
  } else if (typeof filters !== "object") {
    res.status(400).json({ error: '"filters" must be a object' });
  }

  const id = uuidv4();

  const params = {
    TableName: WORKFLOWS_TABLE,
    Item: {
      id,
      workspaceId,
      dateCreated: (new Date()).toUTCString(),
      filters,
    },
  };

  try {
    await dynamoDbClient.put(params).promise();
    res.json({ id, workspaceId, filters });
  } catch (error) {
    console.log(error);
    res.status(500).json({ error: "Could not create workflow" });
  }
});

app.post("/queues", async function (req, res) {
  const { workspaceId, attributeName, attributeValue, name } = req.body;

  if (typeof workspaceId !== "string") {
    res.status(400).json({ error: '"workspaceId" must be a string' });
  } else if (typeof name !== "string") {
    res.status(400).json({ error: '"name" must be a string' });
  } else if (typeof attributeName !== "string") {
    res.status(400).json({ error: '"attributeName" must be a string' });
  } else if (typeof attributeValue === "object" || typeof attributeValue === "undefined") {
    res.status(400).json({ error: '"attributeValue" must not be json' });
  } 

  const id = uuidv4();

  let params = {
    TableName: QUEUES_TABLE,
    Item: {
      id,
      workspaceId,
      name,
      dateCreated: (new Date()).toUTCString(),
      attributeName,
      attributeValue,
    },
  };

  try {
    await dynamoDbClient.put(params).promise();

    params = {
      ExpressionAttributeValues: {
        ":workspaceId": {
          S: workspaceId
        },
        ":attributeName": {
          S: attributeName
        },
	":attributeValue": {
          S: attributeValue
	}
      },
      KeyConditionExpression: 'workspaceId = :workspaceId',
      FilterExpression: 'attributeName = :attributeName and attributeValue = :attributeValue',
      ProjectionExpression: 'workerId',
      TableName: WORKER_ATTRIBUTES_TABLE,
    };

    const workers = await dynamoDb.query(params).promise();

    if (workers.Items.length) {
      for (let worker of workers.Items) {
        params = {
          TableName: WORKER_QUEUE_TABLE,
          Key: {
            workerId: worker.workerId.S,
            queueId: id
          },
        };

        await dynamoDbClient.delete(params).promise();

        params = {
          TableName: WORKER_QUEUE_TABLE,
          Item: {
            id: uuidv4(),
            queueId: id,
            workerId: worker.workerId.S,
            workspaceId,
          }
        };
        await dynamoDbClient.put(params).promise();
      }
    }

    res.json({ id, workspaceId, attributeName, attributeValue });
  } catch (error) {
    console.log(error);
    res.status(500).json({ error: "Could not create queue" });
  }
});

app.post("/workspaces", async function (req, res) {
  const { name } = req.body;

  if (typeof name !== "string") {
    res.status(400).json({ error: '"name" must be a string' });
  }

  const id = uuidv4();

  const params = {
    TableName: WORKSPACES_TABLE,
    Item: {
      id,
      dateCreated: (new Date()).toUTCString(),
      name,
    },
  };

  try {
    await dynamoDbClient.put(params).promise();
    res.json({ name, id });
  } catch (error) {
    console.log(error);
    res.status(500).json({ error: "Could not create workspace" });
  }
});

app.use((req, res, next) => {
  return res.status(404).json({
    error: "Not Found",
  });
});

const workerStreamConsumer = (event, context, callback) => {
  console.log(JSON.stringify(event, null, 2));

  event.Records.forEach(async function(record) {
    if (record.dynamodb.Keys.id.S === "false") {
      return true;
    }
    console.log(record.eventID);
    console.log(record.eventName);
    console.log('DynamoDB Record: %j', record.dynamodb);

    try {
      params = {
        ExpressionAttributeValues: {
          ":workspaceId": {
            S: record.dynamodb.Keys.workspaceId.S
          },
          ":workerId": {
            S: record.dynamodb.Keys.id.S,
          },
        },
        //KeyConditionExpression: 'workspaceId = :workspaceId',
        FilterExpression: 'workerId = :workerId and workspaceId = :workspaceId',
        ProjectionExpression: 'queueId',
        TableName: WORKER_QUEUE_TABLE,
      };

      const queues = await dynamoDb.scan(params).promise();

      if (!queues.Items.length) return true;

      let index = 0;
      let queueIdIndexes = [];

      const ExpressionAttributeValues = {
        ":workspaceId": {
          S: record.dynamodb.Keys.workspaceId.S
        },
        ":currentStatus": {
          S: "pending"
        }
      };

      for (let queue of queues.Items) {
        const queueIdMarker = `:queueId${index}`;
        ExpressionAttributeValues[queueIdMarker] = { S: queue.queueId.S };
        queueIdIndexes.push(queueIdMarker);
        index++;
      }

      params = {
        ExpressionAttributeValues,
        KeyConditionExpression: 'workspaceId = :workspaceId',
        FilterExpression: `queueId IN (${ queueIdIndexes.join(',') }) and currentStatus = :currentStatus`,
        ProjectionExpression: 'id',
        TableName: TASKS_TABLE,
      };

      const data = await dynamoDb.query(params).promise();
      console.log(data);
      if (data && data.Count) {
	for (let item of data.Items) {
          let params = {
            TableName: RESERVATIONS_TABLE,
            Item: {
              id: uuidv4(),
              workerId: record.dynamodb.Keys.id.S,
              workspaceId: record.dynamodb.Keys.workspaceId.S,
              dateCreated: (new Date()).toUTCString(),
              taskId: item.id.S,
              currentStatus: "reserved",
              timeout: (Math.floor(Date.now() / 1000) + 60).toString(),
            },
          };
          await dynamoDbClient.put(params).promise();

          params = {
            TableName: WORKERS_TABLE,
            Key: {
              workspaceId: record.dynamodb.Keys.workspaceId.S,
              id: record.dynamodb.Keys.id.S,
            },
            UpdateExpression: 'set isAvailable = :isAvailable',
            ExpressionAttributeValues: {
             ':isAvailable': "false",
            }
          };
          await dynamoDbClient.update(params).promise();

          params = {
            TableName: TASKS_TABLE,
            Key: {
              workspaceId: record.dynamodb.Keys.workspaceId.S,
              id: item.id.S,
            },
            UpdateExpression: 'set currentStatus = :currentStatus',
            ExpressionAttributeValues: {
             ':currentStatus': "reserved",
            }
          };
          await dynamoDbClient.update(params).promise();
	}
      }
    } catch (error) {
      console.log(error);
    }
  });
  callback(null, "message");
};

const reservationStreamConsumer = (event, context, callback) => {
  console.log(JSON.stringify(event, null, 2));

  event.Records.forEach(async function(record) {
    console.log(record.eventID);
    console.log(record.eventName);
    console.log('DynamoDB Record: %j', record.dynamodb);
    let params;

    switch (record.dynamodb.NewImage.currentStatus.S) {
      case 'reserved':
        params = {
          TableName: RESERVATIONS_TIMEOUT_TABLE,
          Item: {
            id: uuidv4(),
            workerId: record.dynamodb.NewImage.workerId.S,
            workspaceId: record.dynamodb.NewImage.workspaceId.S,
            dateCreated: (new Date()).toUTCString(),
            taskId: record.dynamodb.NewImage.taskId.S,
            reservationId: record.dynamodb.NewImage.id.S,
            currentStatus: "reserved",
            ttl: +record.dynamodb.NewImage.timeout.S,
            timeout: +record.dynamodb.NewImage.timeout.S,
          },
        };
        await dynamoDbClient.put(params).promise();
        break;
      case 'accepted':
      case 'rejected':
        params = {
          TableName: TASKS_TABLE,
          Key: {
            workspaceId: record.dynamodb.NewImage.workspaceId.S,
            id: record.dynamodb.NewImage.taskId.S,
          },
          UpdateExpression: 'set currentStatus = :currentStatus',
          ExpressionAttributeValues: {
           ':currentStatus': record.dynamodb.NewImage.currentStatus.S === "accepted" ? "accepted" : "pending",
          }
        };
        await dynamoDbClient.update(params).promise();
        break;
    }
  });
  callback(null, "message");
};

const reservationTimeoutStreamConsumer = (event, context, callback) => {
  console.log(JSON.stringify(event, null, 2));

  event.Records.forEach(async function(record) {
    console.log(record.eventID);
    console.log(record.eventName);
    console.log('DynamoDB Record: %j', record.dynamodb);

    if (record.eventName !== 'REMOVE') {
      return true;
    }

    let params = {
      TableName: TASKS_TABLE,
      Key: {
        workspaceId: record.dynamodb.OldImage.workspaceId.S,
        id: record.dynamodb.OldImage.taskId.S,
      },
      UpdateExpression: 'set currentStatus = :currentStatus',
      ExpressionAttributeValues: {
        ':currentStatus': "pending",
      }
    };
    await dynamoDbClient.update(params).promise();

    params = {
      TableName: RESERVATIONS_TABLE,
      Key: {
        workspaceId: record.dynamodb.OldImage.workspaceId.S,
        id: record.dynamodb.OldImage.reservationId.S,
      },
      UpdateExpression: 'set currentStatus = :currentStatus',
      ExpressionAttributeValues: {
        ':currentStatus': 'timedout',
      }
    };
    await dynamoDbClient.update(params).promise();
  });
  callback(null, "message");
};

const removeTimedoutReservations = async (event, context, callback) => {
  let secs = 0;

  while (secs < 60) {
    let params = {
      ProjectionExpression: 'id',
      TableName: WORKSPACES_TABLE,
    };
    const workspaces = await dynamoDb.scan(params).promise();

    if (!workspaces.Items.length) return true;
    console.log(workspaces.Items);

    for (let workspace of workspaces.Items) {
      params = {
        ExpressionAttributeValues: {
          ":timeout": Math.floor(Date.now() / 1000),
        },
        TableName: RESERVATIONS_TIMEOUT_TABLE,
        Key: {
          workspaceId: workspace.id.S,
        },
	ConditionExpression: "timeout <= :timeout"
      };
      await dynamoDbClient.delete(params).promise();    
    }

    await delayTime(secs);
    secs += 15;
    console.log(`${secs} run: ${new Date()}`);
  }
};

const delayTime = secs => new Promise((res, rej) => setTimeout(() => res(1), secs * 1000));

module.exports.handler = serverless(app);
module.exports.workerStreamConsumer = workerStreamConsumer;
module.exports.reservationStreamConsumer = reservationStreamConsumer;
module.exports.reservationTimeoutStreamConsumer = reservationTimeoutStreamConsumer;
module.exports.removeTimedoutReservations = removeTimedoutReservations;
