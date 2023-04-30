const { jsonCopy } = require("gxlg-utils").json;
const fs = require("fs");

class AsyncTable {
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

  async tick(){
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
        if(!this.jobs.some(j =>
          ["put", "newEntry", "perform"].includes(j.task)
        ))
          this.save();
        job.done(true);
      } else if(job.task == "put"){
        const entry = job.entry;
        const params = job.params;
        const e = this.data[entry] ?? this.types.list.map(
          t => jsonCopy(this.types.defaults[t] ?? null)
        );
        for(const type in params)
          e[this.types.index[type]] = params[type];
        this.data[entry] = e;

        if(!this.jobs.some(j =>
          ["put", "newEntry", "perform"].includes(j.task)
        ))
          this.save();
        job.done(true);
      } else if(job.task == "getEntry"){
        const entry = job.entry;
        const obj = { };
        for(const type of this.types.list)
          obj[type] = jsonCopy(
            this.data[entry]?.[this.types.index[type]] ??
            this.types.defaults[type] ?? null
          );
        job.done(obj);
      } else if(job.task == "get"){
        const entry = job.entry;
        const param = job.params;
        const d = (
          this.data[entry]?.[this.types.index[param]] ??
          this.types.defaults[param] ?? null
        );
        job.done(jsonCopy(d));
      } else if(job.task == "entries"){
        job.done(Object.keys(this.data));
      } else if(job.task == "perform"){

        const entry = job.entry;
        const obj = { };
        for(const type of this.types.list)
          obj[type] = jsonCopy(
            this.data[entry]?.[this.types.index[type]] ??
            this.types.defaults[type] ?? null
          );
        const ret = await job.params(obj);
        const list = [...Array(this.types.list.length)];
        for(const type of this.types.list){
          list[this.types.index[type]] =
            obj[type] ?? this.types.defaults[type] ?? null;
        }
        this.data[entry] = jsonCopy(list);

        if(!this.jobs.some(j =>
          ["put", "newEntry", "perform"].includes(j.task)
        ))
          this.save();
        job.done(ret);
      }
    } else {
      if(this.finish != null){
        this.kill();
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

  async perform(name, callback){
    return new Promise((res, rej) => {
      this.jobs.push({
        "task": "perform",
        "entry": name,
        "params": callback,
        "done": res
      });
    });
  }

}

class AsyncSet {
  constructor(path){
    this.path = path;
    if(!fs.existsSync(path)){
      this.data = new Set();
      this.save();
    } else {
      const d = JSON.parse(fs.readFileSync(path, "utf8"));
      this.data = new Set(d);
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
    return JSON.stringify([...this.data]);
  }

  tick(){
    if(this.locked) return;
    this.locked = true;

    if(this.jobs.length){
      const job = this.jobs.splice(0, 1)[0];
      if(job.task == "add"){
        this.data.add(job.entry);
        if(!this.jobs.some(j => ["add", "remove"].includes(j.task)))
          this.save();
        job.done(true);
      } else if(job.task == "remove"){
        this.data.delete(job.entry);
        if(!this.jobs.some(j => ["add", "remove"].includes(j.task)))
          this.save();
        job.done(true);
      } else if(job.task == "has"){
        job.done(this.data.has(job.entry));
      } else if(job.task == "size"){
        job.done(this.data.size);
      }
    } else {
      if(this.finish != null){
        this.kill();
        this.finish();
      }
    }

    this.locked = false;
  }

  async add(name){
    return new Promise((res, rej) => {
      this.jobs.push({
        "task": "add",
        "entry": name,
        "done": res
      });
    });
  }

  async remove(name){
    return new Promise((res, rej) => {
      this.jobs.push({
        "task": "remove",
        "entry": name,
        "done": res
      });
    });
  }

  async has(name){
    return new Promise((res, rej) => {
      this.jobs.push({
        "task": "has",
        "entry": name,
        "done": res
      });
    });
  }

  async size(){
    return new Promise((res, rej) => {
      this.jobs.push({
        "task": "size",
        "done": res
      });
    });
  }
}

module.exports = { AsyncTable, AsyncSet };
