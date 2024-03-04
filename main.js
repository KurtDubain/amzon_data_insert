const fs = require("fs");
const dayjs = require("dayjs");
const csv = require("csv-parser");
const path = require("path");
const mysql = require("mysql2/promise");
const { list } = require("./list"); //获取所有的搜索词
const directoryPath = path.join(__dirname, "../"); // 父目录路径
// 正式数据库配置
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
//对一个搜索词的多个文件进行操作
const insertFiles = async (keyWord, oldDate) => {
  try {
    const connection = await mysql.createConnection(dbConfig);
    console.log(
      "数据库连接成功",
      dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss")
    );
    //获取读取过的文件
    let cList = await connection.query(
      "select * from memorization_search_words"
    );
    let readList;
    if (cList[0].length == 0) {
      readList = [];
    } else {
      readList = JSON.parse(cList[0][0].desc);
    }
    console.log(
      `${keyWord}关键词中已经插入的文件名:`,
      readList,
      dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss")
    );
    fs.readdir(directoryPath, async (err, files) => {
      if (err) {
        console.log(
          "找不到目录" + err,
          dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss")
        );
        console.log(
          dayjs(new Date()).diff(oldDate, "minute"),
          dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss")
        );
        await connection.end();
        return;
      }
      let nums = 0;
      let storeArr = [];
      let new_files = files.filter((item) => {
        if (!readList.includes(item)) {
          return item;
        } else {
          return;
        }
      });
      //arrs是未读取过的符合文件名格式的列表
      let arrs = new_files.filter((file) =>
        file.match(/^US_热门搜索词_简单_Week_20\d{2}_\d{2}_\d{2}\.csv$/)
      );
      if (arrs.length == 0) {
        console.log(
          "没有需要处理的文件",
          dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss")
        );
        console.log(
          dayjs(new Date()).diff(oldDate, "minute"),
          dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss")
        );
        await connection.end();
        return;
      }
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
              await connection.query(
                "INSERT INTO search_words (`rank`, `desc`, `timestamp`) VALUES ?",
                [batch]
              );
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
                    await insertAndUpdate(
                      connection,
                      keyWord,
                      storeArr,
                      readList,
                      cList[0].length == 0
                    );
                    console.log(
                      dayjs(new Date()).diff(oldDate, "minute"),
                      dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss")
                    );
                    await connection.end();
                  }
                }
                if (ResultSetHeader.affectedRows > 0) {
                  console.log(
                    `${file}文件对应的${keyWord}已经完成新增`,
                    dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss")
                  );
                } else {
                  console.log(
                    "无新增内容",
                    dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss")
                  );
                }
                stream.resume();
              } catch (error) {
                console.log(
                  error,
                  dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss")
                );
              }
            } else {
              console.log(
                "无新增内容",
                dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss")
              );
              nums++;
              storeArr.push(file);
              if (nums == arrs.length) {
                await insertAndUpdate(
                  connection,
                  keyWord,
                  storeArr,
                  readList,
                  cList[0].length == 0
                );
                console.log(
                  dayjs(new Date()).diff(oldDate, "minute"),
                  dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss")
                );
                await connection.end();
              }
            }
          })
          .on("close", () => {});
      });
      try {
        await Promise.all([...promises]);
      } catch (err) {
        console.error("处理文件时候的出现了报错:", err);
      }
    });
  } catch (err) {
    if (err.code === "ETIMEDOUT") {
      console.log(
        "连接超时，请重新连接",
        dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss")
      );
      //骚扰彬哥
    } else {
      console.error(
        "操作数据库的时候出现了报错:",
        err,
        dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss")
      );
    }
  }
};

const insertAndUpdate = async (cn, kw, arr_new, arr_old, isInsert) => {
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
  console.log(res, dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss"));
  return res;
};
const main = () => {
  let date = dayjs(new Date());
  let promises = list.map((item) => {
    console.log(
      `目前所有的搜索词有${list}`,
      dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss")
    );
    console.log(
      `当前处理的搜索词是：${item}！`,
      dayjs(new Date()).format("YYYY-MM-DD HH:mm:ss")
    );
    return insertFiles(item, date);
  });
  Promise.all(promises);
};
main();
