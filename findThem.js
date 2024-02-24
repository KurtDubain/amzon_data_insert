// 处理一个文件夹下的同类型的csv的批量处理和数据插入操作
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
const dbConfig = {
  host: "127.0.0.1",
  port: 3306,
  user: "Amazon",
  password: "Amazon20240221",
  database: "amazon_boot",
  socketPath: "/var/run/mysqld/mysqld.sock",
};
// 测试
// const dbConfig = {
//     host: '43.143.148.172',
//     port: 3306,
//     user: 'neon',
//     password: 'NeonOnTestServer',
//     database: 'amazon_boot'
// };
console.log("开始了~", new Date());
// 输入处理初始化
rl.question("请输入你希望查找的关键词：", async (keyWord) => {
  console.log(`关键词是：${keyWord}！`);
  rl.close();
  try {
    const connection = await mysql.createConnection(dbConfig);
    console.log("Connected successfully to MySQL");
    // 读取文件夹内的全部文件
    fs.readdir(directoryPath, async (err, files) => {
      if (err) {
        console.log("Unable to scan directory: " + err);
        await connection.end();
        return;
      }
      // 利用nums控制何时断开数据库连接
      let nums = 0;
      // 过滤获取合规的文件数量(数组,元素为文件名称)
      let arrs = files.filter((file) =>
        file.match(/^US_热门搜索词_简单_Week_20\d{2}_\d{2}_\d{2}\.csv$/)
      );
      // 组成Promises数组,每个元素为一个文件的处理过程
      const promises = arrs.map((file) => {
        // importCsvData(path.join(directoryPath, file), keyWord, connection)
        // 补丁,用于批量处理数据库插入操作
        let batch = [];
        // 创建文件流处理文件读取操作
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
            // 对于要求合规的元素进行插入操作
            if (desc.includes(keyWord)) {
              batch.push([rank, desc, timestamp]);
            }
            // 如果补丁内容到了1000,执行批量插入处理
            if (batch.length >= 1000) {
              // 注意这里移除了原有的await，因为.on('data')不支持异步函数
              stream.pause();
              // 使用暂停强制最后执行数据库操作
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
            // 用于处理最后一批数据
            // 如果最后一批数据大于0
            if (batch.length > 0) {
              try {
                stream.pause();
                // 同样是暂停插入,放在最后执行
                const results = await connection.query(
                  "INSERT INTO search_words (`rank`, `desc`, `timestamp`) VALUES ?",
                  [batch]
                );
                let [ResultSetHeader] = results;
                // 如果影响的row不为1000,表明这个最后一批处理,执行nums++,表示文件处理数目加一
                if (ResultSetHeader.affectedRows != 1000) {
                  nums++;
                  // 判断nums是否和数组数量一样,如果一样,说明文件处理完毕,断开数据库连接
                  if (nums == arrs.length) {
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
              // 如果最后一批数据为空,则说明正好为空
              // 文件处理数量++,同时判断是否要断开数据库连接
              console.log("No data to insert.");
              nums++;
              if (nums == arrs.length) {
                await connection.end();
              }
            }
          })
          .on("close", () => {
            console.log(111);
          });
      });
      try {
        // 使用Promise.all等待所有文件处理完成
        let res = await Promise.all([...promises]);
        // if (res.length === arrs.length) {
        //   let cRes = await connection.end();
        //   console.log(res, arrs, 'all结束');
        // }
      } catch (err) {
        console.error("An error occurred during file processing:", err);
      }
    });
  } catch (err) {
    console.error("An error occurred:", err);
  }
});
