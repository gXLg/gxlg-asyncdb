const { jsonCopy } = require("gxlg-utils").json;
const fs = require("fs");

class AsyncDB {
  constructor(path, types){
    this.path = path;
    if(!fs.existsSync(path)){
      const t = { "index": { }, "defaults": { }, "list": [] };
      if(types == undefined)
        throw new Error("Types must be specified for a new DB!");
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
      this.data = { };
      this.save();
    } else {
      const d = JSON.parse(fs.readFileSync(path, "utf8"));
      this.types = d.types;
      this.data = d.data;
    }

    this.locked = false;
    this.jobs = [];
    this.ticker = setInterval(() => { this.tick(); }, 1);
    this.finish = null;
  }

  async stop(){
    return new Promise((res, rej) => {
      this.finish = res;
    });
  }

  kill(){
    clearInterval(this.ticker);
  }

  save(){
    fs.writeFileSync(this.path, this.dataString());
  }

  dataString(){
    return JSON.stringify({ "types": this.types, "data": this.data });
  }

  tick(){
    if(this.locked) return;
    this.locked = true;

    if(this.jobs.length){
      const job = this.jobs.splice(0, 1)[0];
      if(job.task == "newEntry"){
        const entry = job.entry;
        const params = job.params;
        const e = [];
        for(const type of this.types.list){
          if(type in params) e.push(params[type]);
          else e.push(this.types.defaults[type] ?? null);
        }
        this.data[entry] = e;
        if(!this.jobs.some(j => ["put", "newEntry"].includes(j.task)))
          this.save();
        job.done(true);
      } else if(job.task == "put"){
        const entry = job.entry;
        const params = job.params;
        const e = this.data[entry];
        for(const type in params)
          e[this.types.index[type]] = params[type];
        if(!this.jobs.some(j => ["put", "newEntry"].includes(j.task)))
          this.save();
        job.done(true);
      } else if(job.task == "getEntry"){
        const entry = job.entry;
        const obj = { };
        for(const type of this.types.list)
          obj[type] = this.data[entry][this.types.index[type]];
        job.done(obj);
      } else if(job.task == "get"){
        const entry = job.entry;
        const param = job.params;
        const d = this.data[entry][this.types.index[param]];
        job.done(d);
      } else if(job.task == "entries"){
        job.done(Object.keys(this.data));
      }
    } else {
      if(this.finish != null){
        clearInterval(this.ticker);
        this.finish();
      }
    }

    this.locked = false;
  }

  async newEntry(name, params){
    return new Promise((res, rej) => {
      this.jobs.push({
        "task": "newEntry",
        "entry": name,
        "params": params,
        "done": res
      });
    });
  }

  async put(name, params){
    return new Promise((res, rej) => {
      this.jobs.push({
        "task": "put",
        "entry": name,
        "params": params,
        "done": res
      });
    });
  }

  async getEntry(name){
    return new Promise((res, rej) => {
      this.jobs.push({
        "task": "getEntry",
        "entry": name,
        "params": null,
        "done": res
      });
    });
  }

  async get(name, param){
    return new Promise((res, rej) => {
      this.jobs.push({
        "task": "get",
        "entry": name,
        "params": param,
        "done": res
      });
    });
  }

  async entries(){
    return new Promise((res, rej) => {
      this.jobs.push({
        "task": "entries",
        "entry": null,
        "params": null,
        "done": res
      });
    });
  }
}

module.exports = AsyncDB;
