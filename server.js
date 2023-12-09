const express = require('express');
const cors = require('cors');
const config = require('./config');
const routes = require('./routes');

const app = express();
app.use(cors({
  origin: '*',
}));

// We use express to define our various API endpoints and
// provide their handlers that we implemented in routes.js
app.get('/all_metros', routes.all_metros);
app.get('/top_zip_codes_by_metro', routes.top_zip_codes_by_metro);
app.get('/all_zip_codes_in_metro', routes.all_zip_codes_in_metro);
app.get('/irs_data_by_zipcode', routes.irs_data_by_zipcode);
app.get('/irs_zillow_data_by_zipcode', routes.irs_zillow_data_by_zipcode);
app.get('/avg_total_income', routes.avg_total_income);
app.get('/author/:type', routes.author);
app.get('/random', routes.random);
app.get('/IRS_income', routes.IRS_income);
app.get('/affordability', routes.affordability);
app.get('/home_value_index', routes.home_value_index);
app.get('/map', routes.map);
app.get('/slider_search', routes.slider_search);
app.get('/statistics', routes.statistics);
app.get('/zip/:zipcode', routes.zipcodeData);


app.listen(config.server_port, () => {
  console.log(`Server running at http://${config.server_host}:${config.server_port}/`)
});

module.exports = app;
