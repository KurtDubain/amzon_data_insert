const csv = require("csv-parser");
const fs = require("fs");
//获取用户输入
const readline = require("readline");
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});
const mysql = require("mysql2/promise");
// 测试
const dbConfig = {
  host: "43.143.148.172",
  port: 3306,
  user: "neon",
  password: "NeonOnTestServer",
  database: "amazon_boot",
};
// 正式
// const dbConfig = {
//     host: '127.0.0.1',
//     port: 3306,
//     user: 'Amazon',
//     password: 'Amazon20240221',
//     database: 'amazon_boot',
//     socketPath: '/var/run/mysqld/mysqld.sock'
// };
let csvFilePath = "";
const batchSize = 1000; // 每批1000条记录

async function importCsvData() {
  console.log("begin", new Date());
  try {
    const connection = await mysql.createConnection(dbConfig);
    await connection.beginTransaction(); // 开始事务
    console.log("MySql连接成功");

    let batch = [];
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on("data", async (row) => {
        let rank;
        let desc;
        let timestamp;
        let i = 0;
        // 简单遍历获取对象键值对方便之后处理
        for (let key in row) {
          let item = row[key];
          if (i == 0) {
            rank = item;
          } else if (i == 1) {
            desc = item;
          } else if (i == 20) {
            timestamp = item;
          }
          i++;
        }
        if (desc.includes(keyWord)) {
          batch.push([rank, desc, timestamp]);
        }
        if (batch.length >= batchSize) {
          connection.query(
            "INSERT INTO search_words (`rank`, `desc`, `timestamp`) VALUES ?",
            [batch]
          );
          batch = []; // 重置批次
        }
      })
      .on("end", async () => {
        if (batch.length > 0) {
          // 处理最后一批数据
          await connection.query(
            "INSERT INTO search_words (`rank`, `desc`, `timestamp`) VALUES ?",
            [batch]
          );
        }
        await connection.commit(); // 提交事务
        console.log("数据已经插入数据库中", new Date());
        await connection.end();
      });
  } catch (err) {
    console.error("出现错误:", err);
  }
}
let keyWord = "";
rl.question("请输入你希望查找的关键词：", (answer) => {
  keyWord = answer;
  console.log(`关键词是：${answer}！`);
  rl.question("请输入你希望查找的文件名：", (answer) => {
    csvFilePath = "../" + answer;
    console.log(`查找的文件名是：${answer}！`);
    rl.close();
    importCsvData();
  });
});
