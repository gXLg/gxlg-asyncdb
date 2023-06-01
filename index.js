const { jsonCopy } = require("gxlg-utils").json;
const fs = require("fs");

class AsyncTable {
  constructor(path, types){
    this.path = path;

    const t = { "index": { }, "defaults": { }, "list": [] };

    let i = 0;
    for(const type of types){
      if(typeof type == "string"){
        t.index[type] = i ++;
        t.list.push(type);
      } else {
        t.index[type[0]] = i ++;
        t.defaults[type[0]] = jsonCopy(type[1]);
        t.list.push(type[0]);
      }
    }

    this.types = t;

    if(fs.existsSync(path))
      this.data = JSON.parse(fs.readFileSync(path, "utf8"));
    else
      this.data = { };

    fs.writeFileSync(path, this.dataString());

    this.locks = { };
    this.save_lock = null;

    return new Proxy(this, {
      "get": (target, key) => {
        return async callback =>
          await target.perform(key, callback);
      }
    });
  }

  async save(){
    const save_lock = this.save_lock;
    const new_lock = new Promise(async res => {
      await save_lock;
      fs.writeFileSync(this.path, this.dataString());
      res();
    });
    this.save_lock = new_lock;
    return new_lock;
  }

  dataString(){
    return JSON.stringify(this.data);
  }

  async perform(key, callback){

    const lock = this.locks[key] ?? null;
    const new_lock = new Promise(async res => {
      await lock;

      const obj = { };
      for(const type of this.types.list)
        obj[type] = jsonCopy(
          this.data[key]?.[this.types.index[type]] ??
          this.types.defaults[type] ?? null
        );
      const ret = await callback(obj);

      const list = [...Array(this.types.list.length)];
      for(const type of this.types.list){
        list[this.types.index[type]] =
          obj[type] ?? this.types.defaults[type] ?? null;
      }
      this.data[key] = jsonCopy(list);
      await this.save();

      res(ret);

    });

    this.locks[key] = new_lock;
    return new_lock;
  }
}


class AsyncSet {
  constructor(path){
    this.path = path;
    if(!fs.existsSync(path)){
      this.data = new Set();
      fs.writeFileSync(path, this.dataString());
    } else {
      const d = JSON.parse(fs.readFileSync(path, "utf8"));
      this.data = new Set(d);
    }

    this.lock = null;
  }

  async save(){
    fs.writeFileSync(this.path, this.dataString());
  }

  dataString(){
    return JSON.stringify([...this.data]);
  }

  async add(name){
    const lock = this.lock;
    const new_lock = new Promise(res => {
      await lock;
      this.data.add(name);
      res();
    });
    this.lock = new_lock;
    return new_lock;
  }

  async remove(name){
    const lock = this.lock;
    const new_lock = new Promise(res => {
      await lock;
      this.data.delete(name);
      res();
    });
    this.lock = new_lock;
    return new_lock;
  }

  async has(name){
    const lock = this.lock;
    const new_lock = new Promise(res => {
      await lock;
      const h = this.data.has(name);
      res(h);
    });
    this.lock = new_lock;
    return new_lock;
  }

  async size(){
    const lock = this.lock;
    const new_lock = new Promise(res => {
      await lock;
      const s = this.data.size;
      res(s);
    });
    this.lock = new_lock;
    return new_lock;
  }

}

module.exports = { AsyncTable, AsyncSet };
