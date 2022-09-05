const tableStore = require("azure-storage");

//2. read config variable
const configVar = require("./configVariable");

//3. read access keys
const authStore = require("./keys");
const authObject = new authStore();
const tableClient = tableStore.createTableService(
    authObject.accountName,
    authObject.accessKey
  );

////////////////////////
//GENERAL FUNCTION//////
////////////////////////
function CreateTableIfNotExists(AzureTableName){
  return new Promise(resolve => {
      tableClient.createTableIfNotExists(AzureTableName, (error, result) => {
        if (error) {
          // context.log(`[CreateTableIfNotExists] Log : Error Occured in table creation ${error.message}`);
          resolve(`ERROR`);
        } else {
          // context.log( `[CreateTableIfNotExists] Log : Result Table create success ${result.TableName} \n\n ${result.created}`);
          resolve(result.TableName);
        }
      });
  });
}

function QueryDataByPartitionKeyAndRowKey(tableName, partitionKey, rowKey){
  return new Promise(resolve => {
      tableClient.retrieveEntity(tableName, partitionKey, rowKey, (error, result, resp) => {
          if (error) {
            resolve(error.message);
          } else {
            // // context.log(`[QueryDataByPartitionKeyAndRowKey] LOG result: ${JSON.stringify(result)} `);
            resolve(resp.body);
          }
        }
      );
  });
} 

function QueryDataByPartitionKey(tableName, partitionKey){
  return new Promise(resolve => {
    var query = new tableStore.TableQuery().where("PartitionKey eq ?",partitionKey);
    tableClient.queryEntities(tableName, query, null, (error, result, resp) => {
      if (error) {
        // response.send(`Error Occured in table creation ${error.message}`);
        resolve({ statusCode: 400, data: error.message });
      } else {
        resolve({ statusCode: 200, data: resp.body.value });
      }
    });
  });
}


//Get Customer Table name from Table Master by datetime
function GetTableName( category, datetime){
  return new Promise(async(resolve) => {
    let tableName = '';
    try {
      datetime = (datetime)?datetime : new Date();
      tableName = configVar.configTable[category]
      if (category == "customerDuration") {
        let yearName = datetime.getFullYear();
        if (datetime.getMonth() < 10) {
          yearName += "0" + String(datetime.getMonth() + 1);
        } else {
          yearName += String(datetime.getMonth() + 1);
        }
        tableName +=yearName;
      }
    }catch(err){
    }
    let checkTable = await CreateTableIfNotExists(tableName);
    resolve(tableName)
  });
}

//Get Customer Table name of customer history by datetime
function GetCustomerDurationTable(datetime){
  return new Promise(async(resolve) => {
    // let datetime = new Date(2022,1);

    let tableCategory = `customerDuration`;
    let customerDurationTable = await  GetTableName(tableCategory,datetime);
    checkTable = await  CreateTableIfNotExists(customerDurationTable);
    // // context.log(`[GetCustomerDurationTable] LOG : ${customerDurationTable} `);
    resolve(customerDurationTable)
  });
}
/////////////////////////////////////////////

function GetDeviceLocationByDeviceId(deviceId){
  return new Promise(async(resolve) => {
    try {
      let queryLocation = await QueryDataByPartitionKeyAndRowKey(configVar.configTable['deviceManagement'], deviceId,'assignment');//
      // let locId = queryLocation.LocationID;
      resolve(queryLocation);
    }catch(error){
      resolve(null);
    }
  });

}

function InsertNewCustomerDurationData(tableName, data){
  return new Promise(async(resolve) => {
    ////SAMPLE DATA
    // data = {
    //   Datetime: new Date(2021,1,15,14,0),
    //   Duration: 4500,
    //   LocationID: 2,
    //   LocationName: 'Jakarta Barat',
    //   DeviceID: 11
    // }
    // // context.log(`data2: ${data.Duration} \n\n ${JSON.stringify(data)}`);
    let PartitionKey = String((data.Datetime).getDate());
    let RowKey = String(Number(data.Datetime));
    let Duration=Number(data.Duration);
    let EndTime = (data.Datetime).toISOString();
    let StartTime = (new Date(Number(data.Datetime)-Duration*1000)).toISOString(); 
    // // context.log(`entity: ${PartitionKey} --${StartTime} -  ${EndTime}`);
    let LocationID=Number(data.LocationID);
    let DeviceID = Number(data.DeviceID);
    
    
    let entity = {
      PartitionKey: PartitionKey,
      RowKey: LocationID+'_'+DeviceID+'_'+RowKey,
      LocationID: LocationID,
      LocationName: data.LocationName,
      DeviceID: DeviceID,
      StartTime: StartTime,
      EndTime: EndTime, 
      Duration: Duration
    };

    // // context.log(`entity: ${entity.StartTime} \n\n ${JSON.stringify(entity)}`);
    tableClient.insertEntity(tableName, entity, (error, result) => {
        if (error) {
          // context.log(`[InsertNewCustomerDurationData] LOG ERROR : ${error.message}`);
          resolve(error.message);
        } else {
          // context.log(`[InsertNewCustomerDurationData] LOG RESULT: ${result}`);
          resolve(result);
        }
    });

  });
}


function UpdateDataCustomer(tableName, partitionKey,rowKey,fieldName, fieldData){
  return new Promise(async(resolve) => {
    // search the reocrd in the table based on the PartitionKey and RowKey
    tableClient.retrieveEntity(tableName, partitionKey, rowKey, (error, result1, resp) => {
        if (error) {
          response.send( `Error Occured while retrieving data record not found ${error.message}`);
        } else {
          // if the record is found then update
          // let prd = {
          //   PartitionKey: partitionKey,
          //   RowKey: rowKey,
          //   datetime: result1.datetime,
          //   duration: duration
          // };
          let prd = resp.body;
          prd[fieldName]=fieldData;
          tableClient.replaceEntity(tableName, prd, (error, result,resp) => {
            if (error) {
              resolve(error.message);
            } else {
              resolve(resp);
            }
          });
        }
    });//end of tableClient.retrieveEntity

  });

}


function InsertDeviceConditionData(tableName, data){
  tableClient.retrieveEntity(tableName, data.PartitionKey, data.RowKey, (error, result, resp) => {
    if (error) {
        tableClient.insertEntity(tableName, data, (errorInsert, resultInsert) => {
            if (errorInsert) {
              // context.log(`[InsertNewCustomerDurationData] LOG ERROR : ${error.message}`);
            //   resolve(errorInsert.message);
            } else {
              // context.log(`[InsertNewCustomerDurationData] LOG RESULT: ${result}`);
            //   resolve(resultInsert);
            }
        });
    } else {
      // if the record is found then update

      //update entity if available
      tableClient.replaceEntity(tableName, data, (errorUpdate, resultUpdate) => {
        if (error) {
            //context.log(`Error Occured during entity update ${errorUpdate.message}`);
        } else {
            //context.log({ statusCode: 200, message: 'update successfull', data: resultUpdate });
        }
    });

    }
});

}

// NOTE COMPLETE - ONGOING
function InsertCustomerDurationData(tableName, data,context){
  return new Promise(async(resolve) => {
    let PartitionKey = String((data.Datetime).getDate());
    let Duration=Number(data.Duration);
    let EndTime = (data.Datetime).toISOString();
    let StartTime =EndTime;// (new Date(Number(data.Datetime)-Duration*1000)).toISOString(); 
    context.log(`entity: ${PartitionKey} --${StartTime} -  ${EndTime}`);
    let LocationID=Number(data.LocationID);
    let DeviceID = Number(data.DeviceID);
    
    let RowKey = LocationID+'_'+DeviceID+'_'+String(data.CustomerNo);
    
    
    let entity = {
      PartitionKey: PartitionKey,
      RowKey: RowKey,
      LocationID: LocationID,
      LocationName: data.LocationName,
      DeviceID: DeviceID,
      StartTime: StartTime,
      EndTime: EndTime, 
      Duration: Duration
    };
    context.log(`entity: ${JSON.stringify(entity)}`);

    tableClient.retrieveEntity(tableName, PartitionKey, RowKey, (error, result, resp) => {

      if (error) {
        context.log(`[UpdateCustomerDurationData] error retrieveEntity : ${error.message}`);
        // resolve(error.message)
        tableClient.insertEntity(tableName, entity, (errorInsert, resultInsert) => {
            if (errorInsert) {
              context.log(`[UpdateCustomerDurationData] ERROR insertEntity : ${error.message}`);
              resolve(errorInsert.message);
            } else {
              context.log(`[UpdateCustomerDurationData] new data has been successfuly inserted : ${JSON.stringify(resultInsert)}`);
              resolve(resultInsert);
            }
        });
      } else {
        context.log(`[UpdateCustomerDurationData] Entity found : ${JSON.stringify(resp.body)}`)
        let newDuration = Number((new Date(entity.EndTime)) - (new Date(resp.body.StartTime)) );
        // context.log(`[UpdateCustomerDurationData] newDuration  : ${Math.floor(newDuration/1000)}`);
        entity.StartTime = (new Date(resp.body.StartTime)).toISOString();
        entity.Duration = Math.floor(newDuration/1000);
        // resolve(resp.body)
        //update entity if available
        tableClient.replaceEntity(tableName, entity, (errorUpdate, resultUpdate) => {
          if (error) {
              context.log(`[UpdateCustomerDurationData] Error Occured during entity update ${errorUpdate.message}`);
              resolve(errorUpdate.message);
          } else {
              context.log(`[UpdateCustomerDurationData] data has been successfuly updated: ${JSON.stringify(resultUpdate)}`);
              resolve(resultUpdate);
          }
        });
  
      }
    });

  });
}

function QueryDataByDate(tableName, year, month, date){
  return new Promise(resolve => {
      let startTime, endTime;
      if (date>0){
        startTime= new Date(year,month,date).toISOString();
        endTime= new Date(year,month,date+1).toISOString();
      } else {
        startTime= new Date(year,month,1).toISOString();
        endTime= new Date(year,month+1,1).toISOString();
      }
     
      let filter = "datetime ge datetime'"+startTime+"' and datetime le datetime'"+endTime+"' ";
      // context.log('QueryData'+ filter);
      var query = new tableStore.TableQuery().where(filter);//("datetime eq ?", 600); .top(5)
      tableClient.queryEntities(tableName, query, null, function (error, result, resp) {
          if (error) {
            // query was unsuccessful
            // context.log(`Error Occured while retrieving data ${error.message}`);
            resolve(error.message);
          } else {
            // query was successful
          //   // context.log(`Success retrieving data ${JSON.stringify(resp.body.value)}`);
            resolve(resp.body.value);
          }
      });
  });
  
}

function QueryDataFromTable(tableName, filter, context){
  return new Promise(resolve => {
    // filter = "datetime ge datetime'"+startTime+"' and datetime le datetime'"+endTime+"' ";
    context.log(`[QueryDataFromTable] from ${tableName} - filter : ${filter}`);
    var query = new tableStore.TableQuery().where(filter);//("datetime eq ?", 600); .top(5)
    tableClient.queryEntities(tableName, query, null, function (error, result, resp) {
        if (error) {
          // query was unsuccessful
          // context.log(`[QueryDataFromTable] Error Occured while retrieving data ${error.message}`);
          resolve(error.message);
        } else {
          // query was successful
          // context.log(`[QueryDataFromTable] Success retrieving data ${JSON.stringify(resp.body.value)}`);
          resolve(resp.body.value);
        }
    });
  });
  
}

module.exports = {
  CreateTableIfNotExists,
  QueryDataByPartitionKeyAndRowKey,
  QueryDataByPartitionKey,
  QueryDataByKey,
  GetTableName,
  GetCustomerDurationTable,
  InsertNewCustomerDurationData,
  UpdateDataCustomer,
  InsertDeviceConditionData,
  InsertCustomerDurationData,
  QueryDataByDate,
  QueryDataFromTable
}