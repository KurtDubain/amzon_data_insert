const csv = require('csv-parser');
const fs = require('fs');
// const mysql = require('mysql2/promise');

// const dbConfig = {
//   host: 'localhost',
//   user: 'yourUsername',
//   password: 'yourPassword',
//   database: 'yourDatabase'
// };

const csvFilePath = './1.csv';
// const batchSize = 1000; // 每批1000条记录

async function importCsvData() {
  try {
    // const connection = await mysql.createConnection(dbConfig);
    // await connection.beginTransaction(); // 开始事务
    console.log("Connected successfully to MySQL");

    let batch = [];
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on('data', (row) => {
        const rank = row['报告范围=["每日"],选择日期=["2024-02-07"]'];
        // const rank = row['搜索频率排名']
        const desc = row['_1'];
        const timestamp = row['_20'];
        batch.push([rank, desc, timestamp]);
        console.log(row)
        // console.log(row)
        // return
        // if (batch.length >= batchSize) {
        //   connection.query(
        //     'INSERT INTO keyword_list (rank, desc, timestamp) VALUES ?',
        //     [batch]
        //   );
        //   batch = []; // 重置批次
        // }
      })
      .on('end', async () => {
        console.log(batch)
        // if (batch.length > 0) {
          // 处理最后一批数据
        //   await connection.query(
        //     'INSERT INTO keyword_list (rank, desc, timestamp) VALUES ?',
        //     [batch]
        //   );
        // }
        // await connection.commit(); // 提交事务
        // console.log("Data has been successfully inserted into MySQL");
        // await connection.end();
      });
  } catch (err) {
    console.error("An error occurred:", err);
    await connection.rollback(); // 回滚事务
  }
}

importCsvData();
