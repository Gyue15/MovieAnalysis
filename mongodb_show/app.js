const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');
const http = require('http');   

const hostname ='192.168.1.200';
// const hostname ='172.19.117.184';
const port = 8888;   

// Connection URL
const url = 'mongodb://root:mongo_admin_czsA68g@192.168.1.200:27017';
// const url = 'mongodb://root:mongo_admin_czsA68g@172.19.117.184:27017';

// Database Name
const dbName = 'test';

// 图1  柱图  该月 top 电影数据   
// @docs：  (降序排序)
// [{"time":"2019-10","name":"我和我的祖国","total_box":343434,"online_box":232323},
//  {"time":"2019-10","name":"我和我的祖国","total_box":232323,"online_box":121212},...]
const pic1 =  function(callback){
    const client = new MongoClient(url, {useNewUrlParser:true});
    client.connect(function(err) {
        assert.equal(null, err);
        const db = client.db(dbName);
        //该图对应的collection
                
        // 以下代码根据不同功能重新实现
        const collection = db.collection('film_box');
        
        //var max = collection.
        //collection.aggregate([
        //    {
        //        '$match': {
        //            'time': {
        //                '$gte': max
        //            }
        //        }
        //    }
        //]).toArray(function(err, docs) {
        //    assert.equal(err, null);
        //    callback(docs);
        //    client.close();
        //});
        
        collection.aggregate([
            {
                '$sort': {'time': -1}
            }, {
                '$group': {
                    '_id': {'name': '$name'}, 
                    'time': {'$first': '$time'}, 
                    'total_box': {'$first': '$total_box'}, 
                    'online_box': {'$first': '$online_box'}
                }
            }, {
                '$sort': {
                    'total_box': -1
                }
            }, {
                '$limit': 20    //前20条
            }
        ]).toArray(function(err, docs) {
            assert.equal(err, null);
            callback(docs);
            client.close();
        });
    });

}

// 图2 柱图 该月 电影类型 数据   
//type_box（type相同时返回time最大的数据），图2
//      先根据type进行查找，type唯一且time最大； 再在子集合中选取total_month_box最大
// @docs：  (降序排序)
// [{"time":"2019-10","type":"动作","total_month_box":343434,"online_month_box":232323},
//  {"time":"2019-10","type":"科幻","total_month_box":232323,"online_month_box":121212},...]
const pic2 =  function(callback){
    const client = new MongoClient(url, {useNewUrlParser:true});
    client.connect(function(err) {
        assert.equal(null, err);
        const db = client.db(dbName);
        //该图对应的collection
                
        // 以下代码根据不同功能重新实现
        const collection = db.collection('type_box');
        collection.aggregate([
            {
                '$sort': {'time': -1}
            }, {
                '$group': {
                    '_id': {'type': '$type'}, 
                    'time': {'$first': '$time'}, 
                    'total_month_box': {'$first': '$total_month_box'}, 
                    'online_month_box': {'$first': '$online_month_box'}
                }
            }, {
                '$sort': {
                    'total_month_box': -1
                }
            }, {
                '$limit': 20    //前20条
            }
        ]).toArray(function(err, docs) {
            assert.equal(err, null);
            callback(docs);
            client.close();
        });
    });
}

// 图3 线图  票房变化数据
// @docs：  ？？时间的控制？？  如何控制每次发送哪一个月的数据？  开始时从哪个数据开始取？   最新插入collection的数据？
// {"time":"2019-10","total_month_box":2323232323,"online_month_box":1212121212}
const pic3 =  function(callback){
    const client = new MongoClient(url, {useNewUrlParser:true});
    client.connect(function(err) {
        assert.equal(null, err);
        const db = client.db(dbName);
        //该图对应的collection
                
        // 以下代码根据不同功能重新实现
        const collection = db.collection('total_box');
        collection.aggregate([
            {
                '$sort': {'time': -1}
            }, {
                '$limit': 1    
            }
        ]).toArray(function(err, docs) {
            assert.equal(err, null);
            callback(docs);
            client.close();
        });
    });
}

// 图4  线图  产地票房变化数据   百分比
// @docs：  
// [{"time":"2019-10","location":"中国","box_percent":21.6},
//  {"time":"2019-10","location":"美国","box_percent":23.6},...]
const pic4 =  function(callback){
    const client = new MongoClient(url, {useNewUrlParser:true});
    client.connect(function(err) {
        assert.equal(null, err);
        const db = client.db(dbName);
        //该图对应的collection
                
        // 以下代码根据不同功能重新实现
        const collection = db.collection('location_box');
        var newestDate;
        collection.aggregate([
            {
                '$sort': {'time': -1}
            }, {
                '$limit': 1    
            }
        ]).toArray(function(err, docs) {
            assert.equal(err, null);
            newestDate = docs[0].time;
            collection.aggregate([
                {
                    '$match': {'time':{'$gte': newestDate}}
                }
            ]).toArray(function(err, docs) {
                assert.equal(err, null);
                callback(docs);
                client.close();
            });
        });
    });
}

// 图5   map图  该月 各省份 电影票房数据 
// @docs：  
// [{"time":"2019-10","province":"北京","province_id":1,"total_month_box":121212},
//  {"time":"2019-10","province":"天津","province_id":2,"total_month_box":121212},...]
const pic5 =  function(callback){
    const client = new MongoClient(url, {useNewUrlParser:true});
    client.connect(function(err) {
        assert.equal(null, err);
        const db = client.db(dbName);
        //该图对应的collection
                
        // 以下代码根据不同功能重新实现
        const collection = db.collection('area_box');
        collection.aggregate([
            {
                '$sort': {'time': -1}
            }, {
                '$limit': 1    
            }
        ]).toArray(function(err, docs) {
            assert.equal(err, null);
            console.log(docs + JSON.stringify(docs));
            var newestDate = docs[0].time;
            collection.aggregate([
                {
                    '$match': {'time':{'$gte': newestDate}}
                }
            ]).toArray(function(err, docs) {
                assert.equal(err, null);
                callback(docs);
                client.close();
            });
        });
    });
}

// 图6 柱图  演员  电影数据   一年变化一次
// @docs：  
// [{"time":"2019-10","actor":"张译","total_year_box":232323,"online_year_box":121212},
//  {"time":"2019-10","actor":"吴京","total_year_box":232323,"online_year_box":121212},...]
const pic6 =  function(callback){
    const client = new MongoClient(url, {useNewUrlParser:true});
    client.connect(function(err) {
        assert.equal(null, err);
        const db = client.db(dbName);
        //该图对应的collection
                
        // 以下代码根据不同功能重新实现
        const collection = db.collection('actor_box');
        collection.aggregate([
            {
                '$sort': {'month': -1}
            }, {
                '$group': {
                    '_id': {'actor': '$actor'}, 
                    'time': {'$first': '$time'}, 
                    'total_year_box': {'$first': '$total_year_box'}, 
                    'online_year_box': {'$first': '$online_year_box'}
                }
            }, {
                '$sort': {
                    'total_year_box': -1  //结果降序
                }
            }, {
                '$limit': 20    //前20条
            }
        ]).toArray(function(err, docs) {
            assert.equal(err, null);
            callback(docs);
            client.close();
        });
    });
}

const myParseUrl = function(url,callback){
    data = {};
    var picID = Number(url.substr(1));
    console.log(url + "|" + picID + typeof picID);
    switch(picID){
        case 1:
           pic1(function(ret){callback(ret);});
           break;
        case 2:
           pic2(function(ret){callback(ret);});
           break;
        case 3:
           pic3(function(ret){callback(ret);});
           break;
        case 4:
           pic4(function(ret){callback(ret);});
           break;
        case 5:
           pic5(function(ret){callback(ret);});
           break;
        case 6:
           pic6(function(ret){callback(ret);});
           break;
    }
    
}
       

const server = http.createServer((request,response)=>{
	response.statusCode=200;
	if(request.url!=='/favicon.ico'){
            response.setHeader("Access-Control-Allow-Origin", "*");
            response.setHeader("Access-Control-Allow-Headers", "Content-Type, Content-Length, Authorization, Accept, X-Requested-With , yourHeaderFeild");
            response.setHeader("Access-Control-Allow-Methods","PUT,POST,GET,DELETE,OPTIONS");
            response.setHeader('Content-Type','text/html;charset=utf-8');

            myParseUrl(request.url, function(data) {
                console.log("createServer:" + JSON.stringify(data)); 
                response.write(JSON.stringify(data), function(err){
                    response.end();
                });
            });
	} 
});

server.listen(port,hostname,()=>{ 
	console.log(`Server running at http://${hostname}:${port}/`);
});
