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




  function GetCurrentDashBoardDataByUsername(username){
    
      return new Promise(async(resolve) => {
        let result = ({
            'todayCustomer':0,
            'weeklyCustomer':0,
            'connectedDevice':0,
            'totalDevice':0
          });
        let filter = '';
        // filter = "datetime ge datetime'"+startTime+"' and datetime le datetime'"+endTime+"' ";
        context.log(`[QueryDataFromTable] from ${tableName} - filter : ${filter}`);
        let endTime = new Date().setHours(0,0,0,0);
        let startTime = new Date(); //cari hari pertama weekly
        let filterSummary = "datetime ge datetime'"+startTime+"' and datetime le datetime'"+endTime+"' ";
      
        let customerSummary = await QueryDataFromTable(configVar.configTable['customerSummary'],filterSummary,context);
      });

  }
  
}

module.exports = {
  CreateTableIfNotExists,
  QueryDataByPartitionKeyAndRowKey,
  QueryDataByPartitionKey,
  GetTableName,
  GetCustomerDurationTable,
  GetDeviceLocationByDeviceId,
  QueryDataByDate,
  QueryDataFromTable
}