const mysql = require('mysql')
const config = require('./config.json')

// Creates MySQL connection using database credential provided in config.json
// Do not edit. If the connection fails, make sure to check that config.json is filled out correctly
const connection = mysql.createConnection({
  host: config.rds_host,
  user: config.rds_user,
  password: config.rds_password,
  port: config.rds_port,
  database: config.rds_db
});
connection.connect((err) => err && console.log(err));


// Route: GET /all_metros
// Get unique metros
const all_metros = async function(req, res) {
  connection.query(`
    SELECT DISTINCT metro
    FROM zillow
    WHERE metro IS NOT NULL
    ORDER BY metro ASC;
  `, (err, data) => {
    if (err || data.length === 0) {
      console.log(err);
      res.json([]);
    } else {
      res.json(data);
    }
  }); 
}

const avg_total_income = async function(req, res) {
  connection.query(`
    SELECT Z.metro, ROUND(AVG(I.AVG_total_income)*1000) AS average_income
    FROM zillow Z
    JOIN irs I ON Z.zipcode = I.zipcode
    GROUP BY Z.metro
    ORDER BY average_income DESC
    LIMIT 10;
  `, (err, data) => {
    if (err || data.length === 0) {
      console.log(err);
      res.json([]);
    } else {
      res.json(data);
    }
  });
}
// Route: GET /top_zip_codes_by_metro
// Get top 5 zip codes by metropolitan area
const top_zip_codes_by_metro = async function(req, res) {
  const metro = req.query.metro;
  const escapedMetro = connection.escape(metro);

  connection.query(`
    SELECT
      l.zipcode,
      l.lat,
      l.lng,
      l.state,
      l.city,
      l.county_name,
      l.population,
      l.pop_density,
      z.metro
    FROM
      location l
    JOIN
      zillow z ON l.zipcode = z.zipcode
    WHERE
      z.metro = ${escapedMetro}
    ORDER BY
      l.pop_density DESC
    LIMIT 5;
  `, (err, data) => {
    if (err || data.length === 0) {
      console.log(err);
      res.json([]);
    } else {
      res.json(data);
    }
  });
}

// Route: GET /irs_data_by_zipcode 
//  Get IRS data for a specific zipcode
const irs_data_by_zipcode = async function (req, res) {
  const zipcode = req.query.zipcode;
  const escapedZipcode = connection.escape(zipcode);

  connection.query(`
    SELECT
      l.zipcode,
      l.lat,
      l.lng,
      l.state,
      l.city,
      l.county_name,
      l.population,
      l.pop_density,
      i.number_of_returns,
      i.AGI,
      i.AVG_AGI,
      i.total_income_amt,
      i.AVG_total_income,
      i.taxable_income_nor,
      i.taxable_income_amt,
      i.AVG_taxable_income,
      z.August as Avg_Home_Price
    FROM
      location l
    JOIN
      irs i ON l.zipcode = i.zipcode
    JOIN
      zillow z ON l.zipcode = z.zipcode
    WHERE
      l.zipcode = ${escapedZipcode};
  `, (err, data) => {
    if (err || data.length === 0) {
      console.log(err);
      res.json([]);
    } else {
      res.json(data); 
    }
  });
}

// Route: GET /irs_zillow_data_by_zipcode
//  Get IRS and Zillow data for a specific zipcode
const irs_zillow_data_by_zipcode = async function (req, res) {
  const zipcode = req.query.zipcode;
  const escapedZipcode = connection.escape(zipcode);
  connection.query(`
  WITH ZillowAugust AS (
    SELECT
        zipcode,
        August
    FROM
        zillow
    WHERE
        zipcode = ${escapedZipcode}
  )
  SELECT
      l.zipcode,
      l.lat,
      l.lng,
      l.state_abbreviation AS state,
      l.city,
      l.county_name,
      l.population,
      l.pop_density,
      i.number_of_returns,
      i.AGI,
      i.AVG_AGI,
      i.total_income_amt,
      i.AVG_total_income,
      i.taxable_income_nor,
      i.taxable_income_amt,
      i.AVG_taxable_income,
      z.August AS Avg_Home_Price
  FROM
      location l
  JOIN
      irs i ON l.zipcode = i.zipcode
  JOIN
      ZillowAugust z ON l.zipcode = z.zipcode
  WHERE
      l.zipcode = ${escapedZipcode};
  `, (err, data) => {
    if (err || data.length === 0) {
      console.log(err);
      res.json([]);
    } else {
      res.json(data);
    }
  });
}

const all_zip_codes_in_metro = async function(req, res) {
  const metro = req.query.metro;
  const escapedMetro = connection.escape(metro);
  connection.query(`
    SELECT
      l.zipcode,
      l.lat,
      l.lng,
      l.state,
      l.city,
      l.county_name,
      l.population,
      l.pop_density,
      z.metro
    FROM
      location l
    JOIN
      zillow z ON l.zipcode = z.zipcode
    WHERE
      z.metro = ${escapedMetro}
    ORDER BY l.population DESC;
  `, (err, data) => {
    if (err || data.length === 0) {
      console.log(err);
      res.json([]);
    } else {
      res.json(data);
    }
  });
}

const author = async function(req, res) {
  const name = 'Damien Asseya, Mohamed Sanogho, Evan Xue, Martin McDermott';
  const pennKey = 'adamien';

  // checks the value of type the request parameters
  // note that parameters are required and are specified in server.js in the endpoint by a colon (e.g. /author/:type)
  if (req.params.type === 'name') {
    // res.send returns data back to the requester via an HTTP response
    res.send(`Created by ${name}`);
  } else if (req.params.type === 'pennkey') {
    // TODO (TASK 2): edit the else if condition to check if the request parameter is 'pennkey' and if so, send back response 'Created by [pennkey]'
    res.send(`Created by ${pennKey}`);
  } else {
    // we can also send back an HTTP status code to indicate an improper request
    res.status(400).send(`'${req.params.type}' is not a valid author type. Valid types are 'name' and 'pennkey'.`);
  }
}

// Route 2: GET /random
const random = async function(req, res) {
  // you can use a ternary operator to check the value of request query values
  // which can be particularly useful for setting the default value of queries
  // note if users do not provide a value for the query it will be undefined, which is falsey

  // Here is a complete example of how to query the database in JavaScript.
  // Only a small change (unrelated to querying) is required for TASK 3 in this route.
  connection.query(`
    SELECT L.zipcode as zipcode, L.city, L.state_abbreviation, L.population, L.pop_density, August AS Avg_Home_Price, ROUND(AVG_total_income*1000) AS Avg_Income
    FROM location L JOIN zillow Z ON L.zipcode = Z.zipcode
    JOIN irs I ON Z.zipcode = I.zipcode
    ORDER BY Rand() LIMIT 1;
  `, (err, data) => {
    if (err || data.length === 0) {
      // If there is an error for some reason, or if the query is empty (this should not be possible)
      // print the error message and return an empty object instead
      console.log(err);
      // Be cognizant of the fact we return an empty object {}. For future routes, depending on the
      // return type you may need to return an empty array [] instead.
      res.json({});
    } else {
      res.json({
        data
      });
    }
  });
}

const IRS_income = async function(req, res) {
  // TODO (TASK 6): implement a route that returns all albums ordered by release date (descending)
  // Note that in this case you will need to return multiple albums, so you will need to return an array of objects
  connection.query(`
    SELECT I.zipcode
    FROM irs I
    ORDER BY total_income_amt  DESC
    LIMIT 10
  `, (err, data) => {
    if (err || data.length === 0) {
      console.log(err);
      res.json([]);
    } else {
      res.json(data);
    }
  });
}

const affordability = async function(req, res) {
    const page = req.query.page;
    const pageSize = req.query.page_size ? req.query.page_size : 5;
    const offset = (page - 1) * pageSize;
  // TODO (TASK 7): implement a route that given an album_id, returns all songs on that album ordered by track number (ascending)
    connection.query(`
    SELECT zillow.zipcode, location.city, zillow.state, ROUND(zillow.August / (1000*irs.AVG_total_income)) AS Housing_Multiple
    FROM zillow
    JOIN irs ON zillow.zipcode = irs.zipcode
    JOIN location ON irs.zipcode = location.zipcode
    ORDER BY Housing_Multiple DESC
    LIMIT ${pageSize}
    OFFSET ${offset};

  `, (err, data) => {
    if (err || data.length === 0) {
      console.log(err);
      res.json([]);
    } else {
      res.json(data);
    }
  });
}

const home_value_index = async function(req, res) {
  const page = req.query.page;
  // use the ternary (or nullish) operator to set the pageSize based on the query or default to 10
  const pageSize = req.query.page_size ? req.query.page_size : 10;

  if (!page) {
    const query = `
      SELECT location.population, zillow.zipcode, zillow.city, zillow.state, zillow.August
      FROM zillow
      JOIN location ON zillow.zipcode = location.zipcode
      WHERE population > 4000
      ORDER BY August DESC
    `;

    connection.query(query, (err, data) => {
      if (err || data.length === 0) {
        console.log(err);
        res.json([]);
      } else {
        res.json(data);
      }
    });
  } else {
    // TODO (TASK 10): reimplement TASK 9 with pagination
    // Hint: use LIMIT and OFFSET (see https://www.w3schools.com/php/php_mysql_select_limit.asp)
      const offset = (page - 1) * pageSize;

      // Fetch paginated songs ordered by number of plays
      const query = `
        SELECT location.population, zillow.zipcode, zillow.city, zillow.state, zillow.August
        FROM zillow
        JOIN location ON zillow.zipcode = location.zipcode
        WHERE population > 4000
        ORDER BY August DESC
        LIMIT ${pageSize} OFFSET ${offset};
      `;

      connection.query(query, (err, data) => {
        if (err || data.length === 0) {
          console.log(err);
          res.json([]);
        } else {
          res.json(data);
        }
      });
    }
  }

const map = async function(req, res) {
  const latitude = req.query.latitude ?? 79.95;
  const longitude = req.query.population_high ?? 75.19;
    const query = `
      SELECT *
      FROM location
      WHERE lat BETWEEN (${latitude}-0.1) AND (${latitude}+0.1)
      AND lng BETWEEN (${longitude}-0.1) AND (${longitude}+0.1);
    `;

    connection.query(query, (err, data) => {
      if (err || data.length === 0) {
        console.log(err);
        res.json([]);
      } else {
        res.json(data);
      }
    });
};

const slider_search = async function(req, res) {
  const zipcode = req.query.zipcode ?? '';
  const populationLow = req.query.population_low ?? 1;
  const populationHigh = req.query.population_high ?? 140000;
  const popDensityLow = req.query.pop_density_low ?? 0;
  const popDensityHigh = req.query.pop_density_high ?? 70000;
  const avgtotalincomeLow = req.query.avgtotalincome_low ?? 0;
  const avgtotalincomeHigh = req.query.avgtotalincome_high ?? 2000000;
  const HMLow = req.query.hm_low ?? 1;
  const HMHigh = req.query.hm_high ?? 60;
  const augustLow = req.query.august_low ?? 0;
  const augustHigh = req.query.august_high ?? 11000000;
if (zipcode == '') {
  connection.query(`
  SELECT L.zipcode, population, ROUND(pop_density) AS pop_density, ROUND(AVG_total_income*1000) AS per_capita_income, August AS Home_Price, L.city AS City, L.state_abbreviation as State, ROUND(Z.August / (1000*I.AVG_total_income)) AS Housing_Multiple
  FROM location L JOIN zillow Z ON L.zipcode = Z.zipcode
  JOIN irs I ON Z.zipcode = I.zipcode
  WHERE population <= ${populationHigh}
  AND population >= ${populationLow}
  AND pop_density <= ${popDensityHigh}
  AND pop_density >= ${popDensityLow}
  AND AVG_total_income <= ${avgtotalincomeHigh/1000}
  AND AVG_total_income >= ${avgtotalincomeLow/1000}
  AND Z.August / (1000*I.AVG_total_income) <= ${HMHigh}
  AND Z.August / (1000*I.AVG_total_income) >= ${HMLow}
  AND August <= ${augustHigh}
  AND August >= ${augustLow}
  ORDER BY zipcode ASC`,
(err, data) => {
    if (err || data.length === 0) {
      console.log(err);
      res.json({});
    } else {
      console.log(data)
      res.json(data);
    }
  }); 
  } else {
  connection.query(`
  SELECT L.zipcode, population, ROUND(pop_density) AS pop_density, ROUND(AVG_total_income*1000) AS per_capita_income, August AS Home_Price, L.city AS City, L.state_abbreviation as State, ROUND(Z.August / (1000*I.AVG_total_income)) AS Housing_Multiple
  FROM location L JOIN zillow Z ON L.zipcode = Z.zipcode
  JOIN irs I ON Z.zipcode = I.zipcode
  WHERE (population <= ${populationHigh}
  AND population >= ${populationLow}
  AND pop_density <= ${popDensityHigh}
  AND pop_density >= ${popDensityLow}
  AND AVG_total_income <= ${avgtotalincomeHigh/1000}
  AND AVG_total_income >= ${avgtotalincomeLow/1000}
  AND Z.August / (1000*I.AVG_total_income) <= ${HMHigh}
  AND Z.August / (1000*I.AVG_total_income) >= ${HMLow}
  AND August <= ${augustHigh}
  AND August >= ${augustLow})
  AND (Z.zipcode LIKE '%${zipcode}%'
  OR L.city LIKE BINARY '%${zipcode}%' 
  OR L.state_abbreviation LIKE BINARY '%${zipcode}%')
  ORDER BY Z.zipcode ASC`,


  (err, data) => {
    if (err || data.length === 0) {
      console.log(err);
      res.json([]);
    } else {
      console.log(data)
      res.json(data);
    }
  }); 
}
}

const zipcodeData = async function(req, res) {
  try {
    const zipcode = req.params.zipcode; 
    const escapedZipcode = connection.escape(zipcode);
    // Log the escaped zipcode for debugging
    console.log('Escaped Zipcode:', escapedZipcode);
    const query = `
    WITH ZipcodePercentiles AS (
      SELECT
          l.zipcode,
          ROUND((PERCENT_RANK() OVER (ORDER BY l.pop_density))*100) AS pop_density_percentile,
          ROUND((PERCENT_RANK() OVER (ORDER BY i.AVG_total_income))*100) AS income_percentile,
          ROUND((PERCENT_RANK() OVER (ORDER BY z.August))*100) AS Avg_Home_Price_percentile,
          ROUND((PERCENT_RANK() OVER (ORDER BY (z.August / i.AVG_total_income)))*100) AS affordability_percentile
      FROM location l
      JOIN irs i ON l.zipcode = i.zipcode
      JOIN zillow z ON l.zipcode = z.zipcode
    )
    SELECT
        l.zipcode,
        l.lat,
        l.lng,
        l.state,
        l.city,
        l.state_abbreviation,
        l.county_name,
        l.population,
        l.pop_density,
        p.pop_density_percentile,
        i.AVG_total_income,
        p.income_percentile,
        ROUND(i.AVG_total_income * 1000) AS income,
        z.August as Avg_Home_Price,
        p.Avg_Home_Price_percentile,
        z.metro,
        ROUND(z.August / (i.AVG_total_income * 1000), 1) AS affordability,
        p.affordability_percentile
    FROM location l
    JOIN irs i ON l.zipcode = i.zipcode
    JOIN zillow z ON l.zipcode = z.zipcode
    JOIN ZipcodePercentiles p ON l.zipcode = p.zipcode
    WHERE l.zipcode = ?;
    `;

    // Log the SQL query for debugging
    console.log('SQL Query:', query);

    connection.query(query, [zipcode], (err, data) => {
      if (err) {
        console.error('Error executing query:', err);
        res.json([]);
      } else {
        // Log the data for debugging
        console.log('Query Result:', data);

        if (data.length === 0) {
          console.log('No data found');
          res.json([]);
        } else {
          res.json(data);
        }
      }
    });
  } catch (error) {
    console.error('Exception:', error);
    res.json([]);
  }
};

// Route for statistics
const statistics = async function (req, res) {
  // SQL Queries
  const sqlTopIncomeZipCodes = `
      WITH Metros AS (
        SELECT metro, SUM(population) AS Metro_Population
        FROM location L JOIN zillow Z on L.zipcode = Z.zipcode
        WHERE metro IS NOT NULL
        GROUP BY metro
        ORDER BY Metro_Population DESC
        LIMIT 500
        )
        SELECT I.zipcode, L.city, I.state, ROUND(I.AVG_total_income*1000) AS income
        FROM irs I
        JOIN location L ON I.zipcode = L.zipcode
        JOIN zillow Z ON L.zipcode = Z.zipcode
        JOIN Metros M ON Z.metro = M.metro
        GROUP BY Z.metro, AVG_total_income
        ORDER BY AVG_total_income DESC
        LIMIT 100;
     `;
  const sqlBottomIncomeZipCodes = `
     WITH Metros AS (
       SELECT metro, SUM(population) AS Metro_Population
       FROM location L JOIN zillow Z on L.zipcode = Z.zipcode
       WHERE metro IS NOT NULL
       GROUP BY metro
       ORDER BY Metro_Population DESC
       LIMIT 500
       )
       SELECT I.zipcode, L.city, I.state, ROUND(I.AVG_total_income*1000) AS income
       FROM irs I
       JOIN location L ON I.zipcode = L.zipcode
       JOIN zillow Z ON L.zipcode = Z.zipcode
       JOIN Metros M ON Z.metro = M.metro
       GROUP BY Z.metro, AVG_total_income
       ORDER BY AVG_total_income
       LIMIT 100;
    `;

  const sqlMostAffordableZipCodes = `
      WITH Metros AS (
        SELECT metro, SUM(population) AS Metro_Population
        FROM location L JOIN zillow Z on L.zipcode = Z.zipcode
        WHERE metro IS NOT NULL
        GROUP BY metro
        ORDER BY Metro_Population DESC
        LIMIT 500
        )
        SELECT z.zipcode, z.city, z.state, z.August AS home_price
        FROM zillow z
        JOIN Metros M ON z.metro = M.metro
        ORDER BY home_price
        LIMIT 100;
      `;

  const sqlLeastAffordableZipCodes = `
      WITH Metros AS (
        SELECT metro, SUM(population) AS Metro_Population
        FROM location L JOIN zillow Z on L.zipcode = Z.zipcode
        WHERE metro IS NOT NULL
        GROUP BY metro
        ORDER BY Metro_Population DESC
        LIMIT 500
        )
        SELECT z.zipcode, z.city, z.state, z.August AS home_price
        FROM zillow z
        JOIN Metros M ON z.metro = M.metro
        ORDER BY home_price DESC
        LIMIT 100;
      `;

  const sqlExpensiveHousingMarkets = `
    WITH temp AS (
        SELECT I.zipcode, I.total_income_amt, L.city, L.state, L.state_abbreviation
        FROM irs I
        JOIN location L ON I.zipcode = L.zipcode
        )
        SELECT Tbl.zipcode AS zipcode, Tbl.city, Tbl.state, Tbl.state_abbreviation, Tbl.total_income_amt AS income_amt
        FROM temp Tbl
        WHERE (
            SELECT COUNT(*)
            FROM temp Tbl1
            WHERE Tbl1.zipcode = Tbl.zipcode AND Tbl1.total_income_amt >= Tbl.total_income_amt
        ) <= 10
        ORDER BY Tbl.total_income_amt DESC
        LIMIT 100;
      `;

  const sqlExpensiveMetros = `
      WITH Metros AS (
        SELECT metro, SUM(population) AS Metro_Population
        FROM location L JOIN zillow Z on L.zipcode = Z.zipcode
        WHERE metro IS NOT NULL
        GROUP BY metro
        ORDER BY Metro_Population DESC
        LIMIT 250
      )
      SELECT Z.metro,
            ROUND(SUM(Z.August * L.population) / SUM(L.population)) AS metro_home_price,
            M.Metro_Population AS metro_population,
            ROUND(SUM(L.pop_density * L.population) / SUM(L.population)) AS metro_population_density
      FROM location L
      JOIN zillow Z ON L.zipcode = Z.zipcode
      JOIN Metros M ON Z.metro = M.metro
      WHERE Z.metro IS NOT NULL
      GROUP BY Z.metro, M.Metro_Population
      ORDER BY Metro_Home_Price DESC
      LIMIT 100;
    `;

  const sqlInexpensiveMetros = `
    WITH Metros AS (
      SELECT metro, SUM(population) AS Metro_Population
      FROM location L JOIN zillow Z on L.zipcode = Z.zipcode
      WHERE metro IS NOT NULL
      GROUP BY metro
      ORDER BY Metro_Population DESC
      LIMIT 250
    )
    SELECT Z.metro,
          ROUND(SUM(Z.August * L.population) / SUM(L.population)) AS metro_home_price,
          M.Metro_Population AS metro_population,
          ROUND(SUM(L.pop_density * L.population) / SUM(L.population)) AS metro_population_density
    FROM location L
    JOIN zillow Z ON L.zipcode = Z.zipcode
    JOIN Metros M ON Z.metro = M.metro
    WHERE Z.metro IS NOT NULL
    GROUP BY Z.metro, M.Metro_Population
    ORDER BY Metro_Home_Price
    LIMIT 100;
  `;

  // Execute queries and send response
  connection.query(sqlTopIncomeZipCodes, (err, topIncomeResults) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    connection.query(sqlBottomIncomeZipCodes, (err, bottomIncomeResults) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }

      connection.query(sqlMostAffordableZipCodes, (err, affordableResults) => {
        if (err) {
          return res.status(500).json({ error: err.message });
        }

        connection.query(sqlLeastAffordableZipCodes, (err, unaffordableResults) => {
          if (err) {
            return res.status(500).json({ error: err.message });
          }

          connection.query(sqlExpensiveHousingMarkets, (err, expensiveHousingResults) => {
            if (err) {
              return res.status(500).json({ error: err.message });
            }

            connection.query(sqlExpensiveMetros, (err, expensiveMetrosResults) => {
              if (err) {
                return res.status(500).json({ error: err.message });
              }

              connection.query(sqlInexpensiveMetros, (err, inexpensiveMetrosResults) => {
                if (err) {
                  return res.status(500).json({ error: err.message });
                }

                // Send all results in one response
                res.json({
                  topIncomeZipCodes: topIncomeResults,
                  bottomIncomeZipCodes: bottomIncomeResults,
                  mostAffordableZipCodes: affordableResults,
                  leastAffordableZipCodes: unaffordableResults,
                  expensiveHousingMarkets: expensiveHousingResults,
                  expensiveMetros: expensiveMetrosResults,
                  inexpensiveMetros: inexpensiveMetrosResults,
                });
              });
            });
          });
        });
      });
    });
  });
};

module.exports = {
  all_metros,
  top_zip_codes_by_metro,
  all_zip_codes_in_metro,
  irs_data_by_zipcode,
  irs_zillow_data_by_zipcode,
  author,
  random,
  avg_total_income,
  IRS_income,
  affordability,
  home_value_index,
  map,
  slider_search,
  statistics,
  zipcodeData,
}