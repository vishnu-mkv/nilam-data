String.prototype.toTitleCase = function () {
  return this.replace(/\w\S*/g, function (txt) {
    return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
  });
};

const { parse } = require("csv-parse");
const fs = require("fs");
const http = require("http");

let BLOCK = null;
let FILE_PATH = null;
let ACCESS_TOKEN = null;
// delay to int

let DELAY = 500;

let val = process.argv;
for (let i = 2; i + 1 < val.length; i += 2) {
  let key = val[i];
  let value = val[i + 1];

  switch (key) {
    case "--delay":
      DELAY = parseInt(value);
      console.log("Delay: " + value + " ms");
      break;
    case "--block":
      BLOCK = value;
      console.log("Block: " + value);
      break;
    case "--file":
      FILE_PATH = value;
      console.log("File: " + value);
      break;
    default:
      console.log("Ignoring Option " + key);
  }
}

if (!BLOCK || !FILE_PATH || !DELAY) {
  console.log(
    "Please set the options '--block', '--file' are required. --delay is optional"
  );
  process.exit(1);
}

var prompt = require("prompt");

function getToken() {
  return new Promise((resolve, reject) => {
    var schema = {
      properties: {
        password: {
          hidden: true,
          message: "Enter Access token: ",
        },
      },
    };

    //
    // Start the prompt
    //
    prompt.start();

    //
    // Get two properties from the user: name, password
    //
    prompt.get(schema, function (err, result) {
      //
      // Log the results.
      //
      if (err) {
        reject();
      }
      ACCESS_TOKEN = result.password;
      resolve();
    });
  });
}

const IRRIGATION = ["canal", "borewell", "openwell", "bore well", "open well"];

const records = [];
let success = 0,
  failed = 0;
let failedIndexes = {};
const parseErrors = {};

let retryCount = 0;
console.log("Reading file", FILE_PATH);

// Initialize the parser
const parser = parse({});
// Use the readable stream api to consume records
parser.on("readable", function () {
  let record;
  while ((record = parser.read()) !== null) {
    records.push(record);
  }
});

parser.on("end", async function () {
  await getToken();

  if (!ACCESS_TOKEN) {
    console.log("Access token is required");
    process.exit(0);
  }

  console.log("Waiting for all requests to complete");
  await makeCalls(records);

  console.log("Completed.");

  if (
    Object.keys(parseErrors).length === 0 &&
    Object.keys(failedIndexes).length === 0
  ) {
    return;
  }

  console.log("Writing errors to file");
  // write parse erros to csv file
  // parse error is of the form {index: {sno, name, fathername, mobilenumber, reason}}

  let parseErrorCsv =
    "Block - " +
    BLOCK +
    "\n\n" +
    "S no,Name,Father Name,Mobile Number,Reason\n";

  parseErrorCsv =
    parseErrorCsv +
    Object.values(parseErrors)
      .map((e) => {
        return Object.values(e).join(",");
      })
      .join("\n");

  // write failed indexes to csv file
  // failed indexes is of the form {index: {sno, name, fathername, mobilenumber, reason}}

  parseErrorCsv =
    parseErrorCsv +
    "\n\n\n" +
    Object.values(failedIndexes)
      .map((e) => {
        return Object.values(e).join(",");
      })
      .join("\n");

  fs.writeFileSync("." + BLOCK + "-errors.csv", parseErrorCsv);

  console.log("./" + BLOCK + "-errors.csv");

  // process.exit(0);
});

async function makeCalls(records, retrying = false) {
  await createAll([...records]);

  console.log("Success", success);
  console.log("Failed", failed);

  console.log("Parse Error count : ", Object.keys(parseErrors).length);
  console.log("Parse error indexes", Object.keys(parseErrors));

  console.log("Server error count: ", Object.keys(failedIndexes).length);
  console.log("Failed indexes", Object.keys(failedIndexes));

  // if no errors and no failed indexes, exit
}

// takes array of farmers and sends requests to create farmers with a delay of 100ms between each request and then resolves the promise
// when all requests are complete

function createAll(farmers) {
  let sender;
  return new Promise((resolve, reject) => {
    const promises = [];
    let i = 0;

    sender = setInterval(() => {
      if (farmers.length === i) {
        clearInterval(sender);
        resolve(Promise.all(promises));
        return;
      }

      try {
        const data = farmers[i];

        try {
          let farmer = buildFarmer(data, i);
          console.log("Posting farmer", i);
          let promise = makeRequest(farmer, i);
          promises.push(promise);
        } catch (e) {}

        i++;
      } catch (e) {}
    }, DELAY);
  });
}

function makeRequest(farmer, index) {
  var options = {
    host: "asia-south1-nilam-a01df.cloudfunctions.net",
    path: "/widgets/report/create-farmer",
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      authorization: ACCESS_TOKEN,
    },
  };

  return new Promise(async (resolve) => {
    try {
      var req = http.request(options, function (res) {
        console.log(index, "Status: " + res.statusCode);

        res.setEncoding("utf8");
        res.on("data", function (body) {
          // console.log(body);
          if (res.statusCode !== 403) {
            console.log("Invalid access token or permission Denied");
            process.exit(0);
          }
          if (res.statusCode !== 200) {
            failedIndexes[index] = {
              sno: index + 1,
              name: farmer.basicDetails.firstname.data,
              fathername: farmer.basicDetails.fathername.data,
              mobilenumber: farmer.basicDetails.mobilenumber.data,
              reason: body,
            };
            failed++;
          } else success++;
          return resolve();
        });
      });
      req.on("error", function (e) {
        failedIndexes[index] = {
          sno: index + 1,
          name: farmer.basicDetails.firstname.data,
          fathername: farmer.basicDetails.fathername.data,
          mobilenumber: farmer.basicDetails.mobilenumber.data,
          reason: e.message,
        };
        failed++;
        return resolve();
      });
      req.write(JSON.stringify(farmer));
      req.end();
    } catch (e) {
      failedIndexes[index] = {
        sno: index + 1,
        name: farmer.basicDetails.firstname.data,
        fathername: farmer.basicDetails.fathername.data,
        mobilenumber: farmer.basicDetails.mobilenumber.data,
        reason: e.message,
      };
      failed++;
      return resolve();
    }
  });
}

fs.createReadStream(FILE_PATH).pipe(parser);

//Update class

function buildFarmer(farmer, index) {
  try {
    // console.log(farmer);
    if (!farmer) {
      throw new Error("Farmer " + index + " is undefined");
    }

    // if farmer category is not A, B, C, D, A1, AI throw error
    if (
      !["A", "B", "C", "D", "A1", "AI"].includes(farmer[7].toUpperCase().trim())
    ) {
      throw new Error("Farmer NILAM Plan category is not valid - " + farmer[7]);
    }

    // if mobile number is not 10 digits throw error and if it is not a number throw error
    if (farmer[4].length !== 10 || isNaN(farmer[4])) {
      throw new Error("Mobile number is not valid - " + farmer[4]);
    }

    // if farmer category is not small, marginal, ohers throw error
    if (
      !["small", "marginal", "others"].includes(farmer[5].toLowerCase().trim())
    ) {
      throw new Error("Farmer category is not valid - " + farmer[5]);
    }

    // if community is not SC, ST, MBC, GEN throw error
    if (!["SC", "ST", "MBC", "GEN"].includes(farmer[6].toUpperCase().trim())) {
      throw new Error("Farmer community is not valid");
    }

    // check if father name and name are alphabetic
    if (!/^[a-zA-Z ]+$/.test(farmer[1])) {
      throw new Error("Farmer name is not valid");
    }

    if (!/^[a-zA-Z ]+$/.test(farmer[2])) {
      throw new Error("Farmer father name is not valid");
    }

    // check if terrain area is of form 1.1 or 1
    if (!/^(?:[0-9]+\.)*[0-9]$/.test(farmer[10])) {
      throw new Error("Total extent area (Ha) is not valid - " + farmer[10]);
    }

    if (!/^(?:[0-9]+\.)*[0-9]$/.test(farmer[11])) {
      throw new Error("Cultivated area (Ha) is not valid - " + farmer[11]);
    }

    // if income is not a number throw error
    if (isNaN(farmer[18])) {
      throw new Error("Income is not valid - " + farmer[18]);
    }

    // if address is empty or -, _, nil throw error
    if (
      farmer[3].trim() === "" ||
      farmer[3].trim() === "-" ||
      farmer[3].trim() === "_" ||
      farmer[3].trim() === "nil"
    ) {
      throw new Error("Address is not valid - " + farmer[3]);
    }

    // land classification should be ayacut, irrigated, rainfed, others
    if (
      !["ayacut", "irrigated", "rainfed", "others"].includes(
        farmer[13].toLowerCase().trim()
      )
    ) {
      throw new Error("Land classification is not valid - " + farmer[13]);
    }

    // if farmer class is B and source of irrigation is not in IRRiGATION_SOURCES throw error
    if (
      farmer[7].toUpperCase().trim() === "B" &&
      !IRRIGATION.includes(farmer[14].toLowerCase().trim())
    ) {
      throw new Error(
        "Farmer class is B but Source of irrigation is not valid - Irrigation: " +
          farmer[14]
      );
    }

    // if farmer class is A and electricity is not provided throw error
    if (
      farmer[7].toUpperCase().trim() === "A" &&
      farmer[15].toLowerCase().trim() !== "yes"
    ) {
      throw new Error(
        "Farmer class is A but electricity is not provided - electricity: " +
          farmer[15]
      );
    }

    // if farmer class is A1 or AI and land classification is not irrigated throw error
    if (
      (farmer[7].toUpperCase().trim() === "A1" ||
        farmer[7].toUpperCase().trim() === "AI") &&
      farmer[13].toLowerCase().trim() !== "irrigated"
    ) {
      throw new Error(
        "Farmer class is A1 or AI but land classification is not irrigated - land classification: " +
          farmer[13]
      );
    }

    if (farmer[7].toUpperCase() === "AI") {
      //  ascii ` is 60, a is 61
      farmer[7] = "A1";
    }

    const Farmer = {
      basicDetails: {
        state: {
          label: "State",
          data: "Tamil Nadu",
          type: "text",
        },
        firstname: {
          label: "First Name",
          data: farmer[1].split(" ", 1)[0].toTitleCase(),
          type: "text",
        },
        lastname: {
          label: "Last Name",
          data: farmer[1].split(" ", 1)[1]?.toTitleCase() || "",
          type: "text",
        },
        mobilenumber: {
          label: "Mobile Number",
          data: farmer[4],
          type: "text",
        },
        yearlyincome: {
          label: "Yearly Income",
          data: { 2022: farmer[18] },
          type: "map",
        },
        city: {
          label: "City",
          data: "Erode",
          type: "text",
        },
        address: {
          label: "Area",
          data: farmer[3].trim(),
          type: "text",
        },
        farmertype: {
          label: "Farmer Type",
          data: farmer[5].trim().toTitleCase(),
          type: "text",
        },
        fathername: {
          label: "Father Name",
          data: farmer[2].trim().toTitleCase(),
          type: "text",
        },
      },
      farmingDetails: {
        cattleInformation: {
          label: "Cattle information",
          data: getCattles(farmer[16]),
          type: "map",
        },
        cropsCultivated: {
          label: "Cattle information",
          data: getCrops(farmer[12]),
          type: "list",
        },
        salesMethod: {
          data: getSales(farmer[17]),
          label: "Sales Method",
          type: "map",
        },
        treesCultivation: {
          data: {},
          label: "Trees Cultivation",
          type: "map",
        },
        community: {
          label: "Community",
          data: farmer[6],
          type: "text",
        },
      },
      landDetails: {
        blocknumber: {
          label: "Block Number",
          data: BLOCK,
          type: "text",
        },
        ownername: {
          label: "owner Name",
          data: getOwnerName(farmer[1], farmer[7]),
          type: "text",
        },
        surveynumber: {
          label: "Survey Number",
          data: getSurveyNumber(farmer[8]),
          type: "text",
        },
        totalterrianarea: {
          label: "Total Terrian Area",
          data: farmer[10].trim(),
          type: "text",
        },
        terrianareaundercultivation: {
          label: "Terrian Area under Cultivation",
          data: farmer[11].trim(),
          type: "text",
        },
        sourcesofirrigation: {
          label: "Sources of Irrigation",
          data: getIrrigation(farmer[14]),
          type: "list",
        },
        electricity: {
          label: "Electricity",
          data: farmer[15].toLowerCase().trim() == "yes" ? "EB" : "",
          type: "text",
        },
        landClassification: {
          label: "Land Classification",
          data: farmer[13].trim().toTitleCase(),
          type: "text",
        },
        SubdivisionNumber: {
          data: getSubDivisionNumber(farmer[9]),
          label: "Sub Division Numbers",
          type: "list",
        },
      },
      rulebook: [
        {
          passingMark: 10,
          name: "C",
          questions: [
            {
              question: "Arrangement of land ownership document",
              subQuestion: null,
              mark: 10,
              response: farmer[7].toLowerCase() < "d",
            },
          ],
        },
        {
          passingMark: 10,
          name: "B",
          questions: [
            {
              question: "Creation of water source",
              subQuestion: null,
              mark: 10,
              response: farmer[7].toLowerCase() < "c",
            },
          ],
        },
        {
          passingMark: 100,
          name: "A",
          questions: [
            {
              question:
                "Creation of Electricity supply / Solar powered pump system",
              subQuestion: null,
              mark: 100,
              response: farmer[7].toLowerCase() < "b",
            },
          ],
        },
        {
          passingMark: 10,
          name: "A1",
          questions: [
            {
              question: "Creation of micro irrigation facilities",
              subQuestion: null,
              mark: 10,
              response: farmer[7].toLowerCase() < "a",
            },
          ],
        },
      ],
    };
    // console.log(JSON.stringify(Farmer));
    return Farmer;
  } catch (e) {
    parseErrors[index] = {
      sno: index + 1,
      name: farmer[1],
      fathername: farmer[2],
      mobilenumber: farmer[4],
      reason: e.message,
    };
    failed++;
    throw e;
  }
}

function getSurveyNumber(surveyNumber) {
  surveyNumber = surveyNumber.trim();
  if (
    surveyNumber === "-" ||
    surveyNumber === "" ||
    surveyNumber === " " ||
    surveyNumber === "nil"
  ) {
    return [];
  }

  // split by comma and check if all are numbers
  const surveyNumbers = surveyNumber.split(",").map((s) => {
    if (isNaN(s)) {
      throw new Error("Invalid survey number " + s);
    }

    return s.trim();
  });
}

function getCattles(cattle) {
  cattle = cattle.trim();
  if (
    cattle === "" ||
    cattle === "-" ||
    cattle === "_" ||
    cattle.toLowerCase() === "nil"
  )
    return {};

  const ret = {};
  const cattles = cattle.split(",");
  cattles.forEach((cattle) => {
    const [name, count] = cattle.split("-");

    if (!count || count === "") return;

    // if count is not a number, throw error
    if (isNaN(count)) {
      throw new Error("Cattle count is not a number " + cattle);
    }

    ret[name.trim().toTitleCase()] = parseInt(count);
  });
  return ret;
}

function getCrops(crops) {
  crops = crops.trim();

  if (
    crops === "" ||
    crops === "-" ||
    crops.toLowerCase() === "nil" ||
    crops === "_"
  )
    return [];
  return crops.split(",").map((crop) => crop.trim().toTitleCase());
}

function getSales(sales) {
  sales = sales.trim();
  if (
    sales === "" ||
    sales === "-" ||
    sales.toLowerCase() === "nil" ||
    sales === "_"
  )
    return {};

  const ret = {};
  const salesArr = sales.split(",");
  salesArr.forEach((sale) => {
    ret[sale.trim().toTitleCase()] = "";
  });
  return ret;
}

function getOwnerName(name, class_) {
  if (class_ !== "D") return name.trim().toTitleCase();
  return "";
}

function getIrrigation(irrigation) {
  irrigation = irrigation.trim();

  if (
    irrigation === "" ||
    irrigation === "-" ||
    irrigation === "_" ||
    irrigation.toLowerCase() === "nil"
  )
    return [];
  return irrigation.split(",").map((i) => {
    const l = i.toLowerCase().trim();

    // if irrigation is not in list throw error
    if (!IRRIGATION.includes(l)) {
      throw new Error("Invalid Irrigation " + i);
    }

    return i.trim().toTitleCase();
  });
}

function getSubDivisionNumber(subDiv) {
  subDiv = subDiv.trim();

  if (
    subDiv === "" ||
    subDiv === "-" ||
    subDiv.toLowerCase() === "nil" ||
    subDiv === "_"
  )
    return [];
  return subDiv
    .split(",")
    .filter(Boolean)
    .map((i) => {
      // check if subDiv contains alphabets, numbers, dots
      // if yes, return the number trimmed and uppercased
      // else throw error
      const match = i.match(/^(?:[0-9a-zA-Z]+\.)*[0-9a-zA-Z]+$/g);
      if (match) {
        return i.trim().toUpperCase();
      }
      throw new Error("Invalid sub division number " + i);
    });
}
