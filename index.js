const express = require('express');
// var router = express.Router();

const server = app.listen(process.env.PORT || 5501, () => {
    console.log(`chucknorris server started on port: ${server.address().port}`);
  });

  app.get('/hi', (req,res)=>{
    res.send('Hello World');
    })