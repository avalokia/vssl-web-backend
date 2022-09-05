const e = require('express');
var express = require('express');
var router = express.Router();
var AzureFunction = require('../shared_modules/AzureTableStorageFuncWebsite');

/* GET users listing. */
router.get('/', function(req, res, next) {
  res.send('{"body":"test"}');
});

router.get('/test', async function(req, res, next) {
  let data = await AzureFunction.GetTableName('userList');
  res.send(`{"body":${JSON.stringify(data)}}`);
});

router.get('/test2', async function(req, res, next) {
  let username='isrosyaeful';
  let today = new Date();//new Date(2022,3,3);
  let todayConnection = new Date(Number(today)-1000*60*10); //today - 10 minutes
  let nextDay = new Date(today.getFullYear(),today.getMonth(),today.getDate()+1);
  let firstDayOfTheWeek = new Date(today.getFullYear(),today.getMonth(),today.getDate()-today.getDay());
  let lastDayOfTheWeek = new Date(today.getFullYear(),today.getMonth(),today.getDate()-today.getDay()+7);
  let startTime = new Date(today.getFullYear(),today.getMonth(),1,0,0,0);
  let endTime = new Date(today.getFullYear(),today.getMonth()+1,1,0,0,0); //cari hari pertama weekly
  
  let filter = "EndTime ge '"+startTime.toISOString()+"' and EndTime le '"+endTime.toISOString()+"' ";
  let customerTable = await AzureFunction.GetTableName('customerDuration',today);
  let data = await AzureFunction.QueryDataFromTable(customerTable,filter);
  console.log(`${today} - ${todayConnection} - ${firstDayOfTheWeek} - ${lastDayOfTheWeek}`);
  // console.log(`${data} - \n\n${JSON.stringify(data)} - ${lastDayOfTheWeek}`);
  // let data = await AzureFunction.QueryDataByPartitionKey(table,'3');
  let customerOfTheWeek=0;
  let customerOfTheDay=0;
  for (let i=0;i<data.length;i++){
    if (data[i].EndTime>=firstDayOfTheWeek.toISOString() && data[i].EndTime<lastDayOfTheWeek.toISOString()){
      customerOfTheWeek++;
    }
    if (data[i].EndTime>=today.toISOString() && data[i].EndTime<nextDay.toISOString()){
      customerOfTheDay++;
    }
  }


  let deviceTable = await AzureFunction.GetTableName('deviceConnectivity');
  let arrLocId = ['6'];
  let filterDevice = ''
  for (let i=0;i<arrLocId.length;i++){
    if (filterDevice===''){
      filterDevice+=`( PartitionKey eq '${arrLocId[i]}' `;
    } else {
      filterDevice+=` or PartitionKey eq '${arrLocId[i]}' `;
    }
  }
  if (filterDevice!=='') filterDevice+=')';
  let deviceData = [];
  try {
    deviceData = await AzureFunction.QueryDataFromTable(deviceTable,filterDevice);
  }catch(err){
    deviceData = [];
  }
  let connectedDevice = 0;
  for (let i=0;i<deviceData.length;i++){
    if ( deviceData[i].DateTime>=todayConnection.toISOString() ){
      connectedDevice++;
    }
  }

  let result = {
    'filter':filter
    ,'tableName':customerTable
    ,'customerOfTheWeek':customerOfTheWeek
    ,'customerOfTheDay':customerOfTheDay
    ,'filterDevice':filterDevice
    ,'totalDevice':deviceData.length
    ,'connectedDevice':connectedDevice
    ,'data':data
  }
  console.log(`count : ${customerOfTheWeek} \n\n ${JSON.stringify(result)}`);
  res.send(result);
  //   `result : \n 
  //  ${(result)}`);
}
);



router.get("/DeviceConnectivity", async function (req, res, next) {
  let result = {};//data;
  let locationID = '11';
  let deviceID = '11';
  let deviceStatus = 'Online'
  let message = `message ---- locationID: ${locationID}, deviceID: ${deviceID} ,batteryStatus ${deviceStatus}`; //
  console.log(message);
  //////MAIN PROGRAM

  let userLocationTable = await AzureFunction.GetTableName("userLocation");
  console.log(`userLocationTable ${userLocationTable}`);
  let userLocationFilter =`LocationID eq '${locationID}'`;
  let userLocation = await AzureFunction.QueryDataFromTable(userLocationTable,userLocationFilter);
  console.log('userLocation : ' + JSON.stringify(userLocation));
  if (userLocation.length>0){
    let username = userLocation[0].PartitionKey;
    let deviceConnectivityTable = await AzureFunction.GetTableName("deviceConnectivity");
    
    let data = {
      PartitionKey: String(username), 
      RowKey: String(locationID), 
      username: String(username), 
      LocationID: String(locationID),
      DeviceID: String(deviceID),
      DeviceStatus: String(deviceStatus),
      LastConnection: new Date().toISOString()
    };
    console.log(`Data : ${JSON.stringify(data)}`);
    await AzureFunction.InsertDeviceConditionData(deviceConnectivityTable, data);

  } else {
    result = {
      body:'Error, LocationID not Found'
    }
  }
  
  
  res.send(result);
});

module.exports = router; 

