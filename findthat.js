const csv = require('csv-parser');
const fs = require('fs');
const readline = require('readline');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});
const mysql = require('mysql2/promise');

const dbConfig = {
  host: '43.143.148.172',
  port: 3306,
  user: 'neon',
  password: 'NeonOnTestServer',
  database: 'amazon_boot'
};

const csvFilePath = './1.csv';
const batchSize = 1000; // 每批1000条记录

async function importCsvData() {
  try {
    const connection = await mysql.createConnection(dbConfig);
    await connection.beginTransaction(); // 开始事务
    console.log("Connected successfully to MySQL");

    let batch = [];
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on('data', (row) => {
        let rank;
        let desc;
        let timestamp;
        let i = 0
        for (let key in row) {
          let item = row[key]
          if (i == 0) {
            rank = item
          } else if (i == 1) {
            desc = item
          } else if (i == 20) {
            timestamp = item
          }
          i++
        }
        if (desc.includes(keyWord)) {
          batch.push([rank, desc, timestamp]);
        }
        if (batch.length >= batchSize) {
          connection.query(
            'INSERT INTO search_words (rank, desc, timestamp) VALUES ?',
            [batch]
          );
          batch = []; // 重置批次
        }
      })
      .on('end', async () => {
        if (batch.length > 0) {
          // 处理最后一批数据
          await connection.query(
            'INSERT INTO search_words (rank, desc, timestamp) VALUES ?',
            [batch]
          );
          console.log(batch, 'batch');
        }
        await connection.commit(); // 提交事务
        console.log("Data has been successfully inserted into MySQL");
        await connection.end();
      });
  } catch (err) {
    console.error("An error occurred:", err);
    // await connection.rollback(); // 回滚事务
  }
}
let keyWord = ''
rl.question('请输入你希望查找的关键词：', (answer) => {
  keyWord = answer
  console.log(`关键词是：${answer}！`);
  rl.close();
  importCsvData();
});
