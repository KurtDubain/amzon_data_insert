# amzon_data_insert

## 用于方便处理大量合规的csv文件部分数据进入到数据库的

使用方法如下:
- 克隆代码到本地
- 执行`npm i`操作，下载对应node_modules
- 执行`node xx.js`，具体的文件名称需要根据你的需求来操作
  - 主要执行文件有3个，说明如下
  1. `findIt`:单文件csv的处理

  2. `findThem`:处理一个文件夹下的同类型的csv的批量处理和数据插入操作
  3. `main.js`:用于集成it和them,封装在一个持续运行的服务器上,用于定期更新指定关键字的数据


注意：
- 确保你的本地环境配置了node
- 本项目的`node`版本为`18.12.1`，经过测试，至少还支持`nodev21`

 

