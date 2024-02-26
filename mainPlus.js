const fs = require("fs");
const csv = require("csv-parser");
const path = require("path");
const readline = require("readline");
// const { importCsvData } = require('./4insert_test.js')
const mysql = require("mysql2/promise");
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});
const directoryPath = path.join(__dirname, "../"); // 父目录路径
// // 正式数据库配置
// const dbConfig = {
//     host: '127.0.0.1',
//     port: 3306,
//     user: 'Amazon',
//     password: 'Amazon20240221',
//     database: 'amazon_boot',
//     socketPath: '/var/run/mysqld/mysqld.sock'
// };
// 测试
const dbConfig = {
  host: "43.143.148.172",
  port: 3306,
  user: "neon",
  password: "NeonOnTestServer",
  database: "amazon_boot",
};

rl.question("请输入你希望查找的关键词：", async (keyWord) => {
  console.log(`关键词是：${keyWord}！`);
  let oldDate = new Date();
  rl.close();
  try {
    const connection = await mysql.createConnection(dbConfig);
    console.log("Connected successfully to MySQL");
    //获取读取过的文件
    let cList = await connection.query(
      "select * from memorization_search_words"
    );
    let readList;
    // console.log('cList', cList);
    if (cList[0].length == 0) {
      readList = [];
    } else {
      readList = JSON.parse(cList[0][0].desc);
    }
    console.log("readlist", readList);
    fs.readdir(directoryPath, async (err, files) => {
      if (err) {
        console.log("Unable to scan directory: " + err);
        await connection.end();
        return;
      }
      let nums = 0;
      let storeArr = [];
      //
      let new_files = files.filter((item) => item !== item.includes(readList));
      //arrs是未读取过的符合文件名格式的列表
      let arrs = new_files.filter((file) =>
        file.match(/^US_热门搜索词_简单_Day_20\d{2}_\d{2}_\d{2}\.csv$/)
      );
      const promises = arrs.map((file) => {
        let batch = [];
        let stream = fs
          .createReadStream(path.join(directoryPath, file))
          .pipe(csv());
        stream
          .on("data", async (row) => {
            let rank, desc, timestamp;
            let i = 0;
            for (let key in row) {
              let item = row[key];
              if (i == 0) rank = item;
              else if (i == 1) desc = item;
              else if (i == 20) timestamp = item;
              i++;
            }
            if (desc.includes(keyWord)) {
              batch.push([rank, desc, timestamp]);
            }
            if (batch.length >= 1000) {
              // 注意这里移除了原有的await，因为.on('data')不支持异步函数
              stream.pause();
              let res = await connection.query(
                "INSERT INTO search_words (`rank`, `desc`, `timestamp`) VALUES ?",
                [batch]
              );
              // console.log(res);
              stream.resume();
              batch = []; // 重置批次
            }
          })
          .on("end", async () => {
            if (batch.length > 0) {
              try {
                stream.pause();
                const results = await connection.query(
                  "INSERT INTO search_words (`rank`, `desc`, `timestamp`) VALUES ?",
                  [batch]
                );
                let [ResultSetHeader] = results;
                if (ResultSetHeader.affectedRows != 1000) {
                  nums++;
                  storeArr.push(file);
                  if (nums == arrs.length) {
                    //instert
                    console.log(storeArr);
                    let well = await insertAndUpdate(
                      connection,
                      keyWord,
                      storeArr,
                      readList,
                      cList[0].length == 0
                    );
                    console.log(well);
                    console.log(
                      "时间差",
                      new Date().getTime() - oldDate.getTime()
                    );
                    await connection.end();
                  }
                }
                // console.log(results)
                if (ResultSetHeader.affectedRows > 0) {
                  console.log(
                    "Data has been successfully inserted for",
                    path.join(directoryPath)
                  );
                } else {
                  console.log("No rows were inserted.");
                }
                stream.resume();
              } catch (error) {
                console.log(error);
              }
            } else {
              console.log("No data to insert.");
              nums++;
              storeArr.push(file);
              if (nums == arrs.length) {
                //instert
                console.log(storeArr);
                let well = await insertAndUpdate(
                  connection,
                  keyWord,
                  storeArr,
                  readList,
                  cList[0].length == 0
                );
                console.log(well);
                console.log("时间差", new Date().getTime() - oldDate.getTime());
                await connection.end();
              }
            }
          })
          .on("close", () => {
            console.log(111);
          });
      });
      try {
        let res = await Promise.all([...promises]);
      } catch (err) {
        console.error("An error occurred during file processing:", err);
      }
    });
  } catch (err) {
    console.error("An error occurred:", err);
  }
});

const insertAndUpdate = async (cn, kw, arr_new, arr_old, isInsert) => {
  console.log("isInsert", isInsert);
  let str, res;
  if (isInsert) {
    str = JSON.stringify(arr_new);
    res = cn.query(
      "INSERT INTO memorization_search_words (`key`, `desc`) VALUES (?,?)",
      [kw, str]
    );
  } else {
    str = JSON.stringify([...new Set([...arr_new, ...arr_old])]);
    res = cn.query(
      "update memorization_search_words set `desc`= ? where `key` = ?",
      [str, kw]
    );
  }
  return res;
};
