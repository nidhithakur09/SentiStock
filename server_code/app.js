var express = require('express');
var app = express();
var mysql = require('mysql');
var bodyParser = require('body-parser')

// 
app.use(express.static(__dirname + '/public'));
app.use(bodyParser.urlencoded({extended: false}));

// connect to database
var con = mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "",
    database: "SentiStock"
});

// routes
app.get('/news/:id', function(request, response){
    console.log('GET request received at /news') 
    sentiment = request.params.id
    if(sentiment == "all"){
        con.query("SELECT * FROM SS_News", function (err, result) {
            // console.log(result)
            if (err) throw err;
            else{
                response.send(result);
            }

        });
    }
    else if(sentiment == "positive"){
        con.query("SELECT * FROM SS_News where ss_sentiments='Positive'", function (err, result) {
            // console.log(result)
            if (err) throw err;
            else{
                response.send(result);
            }

        });
    }
    else if(sentiment == "negative"){
        con.query("SELECT * FROM SS_News where ss_sentiments='Negative'", function (err, result) {
            // console.log(result)
            if (err) throw err;
            else{
                response.send(result);
            }

        });
    }
    else if(sentiment == "neutral"){
        con.query("SELECT * FROM SS_News where ss_sentiments='Neutral'", function (err, result) {
            // console.log(result)
            if (err) throw err;
            else{
                response.send(result);
            }

        });
    }
});

app.get('/symbol/:id', function(req,res){
    console.log("GET request received at /symbol")
    res.sendFile('symbol.html', {root:__dirname})
})

app.get('/snews/:id', function(request, response){
    console.log('GET request received at /snews') 
    console.log(request.params.id)
    symbol = request.params.id.toUpperCase()
    var queryString = "SELECT * FROM SS_News where ss_symbol like '%"+symbol+"%'"
    con.query(queryString, function (err, result) {
        console.log(result)
        if (err) throw err;
        else{
            response.send(result);
        }
    });
});

app.get('/', function(req,res){
    res.sendFile('index.html',{root: __dirname })
})

app.get('/sentiment/:id', function(req,res){
    res.sendFile('sentiment.html',{root: __dirname })
    
})

app.get('/login', function(req,res){
    res.sendFile('login.html',{root: __dirname })
    
})

app.get('/slogin/:username/:password', function(req,res){
    var username = req.params.username
    var password  = req.params.password
    console.log(username)
    console.log(password)
    var queryString = "SELECT * FROM SS_User where username = '"+username+"'"
    con.query(queryString, function (err, result) {
        console.log(result)
        if (err) throw err;
        else{
            if(result)
            if(result.length>0 && password==result[0].password){
                res.sendStatus(200);
            }
            else{
                res.sendStatus(403);
            }
        }
    });
    // res.sendStatus(200)
})

app.get('/sregister/:username/:password', function(req,res){
    var username = req.params.username
    var password  = req.params.password
    console.log(username)
    console.log(password)
    var queryString = "SELECT * FROM SS_User where username = '"+username+"'"
    con.query(queryString, function (err, result) {
        console.log(result)
        if (err) throw err;
        else{
            if(result.length>0){
                res.sendStatus(403);
            }
            else{
                var regQuery = "INSERT INTO SS_User values('"+username+"','"+password+"'";
                con.query(regQuery, function(err2,result2){
                    if (err2) throw err2;
                    else{
                        res.sendStatus(200)
                    }
                })
            }
        }
    });
    // res.sendStatus(200)
})

app.get('/register', function(req,res){
    res.sendFile('register.html',{root: __dirname })
    
})

// listen for trafic and display on localhost:9000
app.listen(9000, function () {
    console.log('Connected to port 9000');
});