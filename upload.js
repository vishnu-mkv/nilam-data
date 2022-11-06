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

console.log("\n\nNILAM BULK UPLOAD\n===========================\n");

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
          // hidden: true,
          message: "Enter Access token",
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
      console.log("\n\n\n");
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
  await makeCalls(records);

  console.log("Completed.");

  if (
    Object.keys(parseErrors).length === 0 &&
    Object.keys(failedIndexes).length === 0
  ) {
    return;
  }

  writeError();
  // process.exit(0);
});

function writeError() {
  console.log(
    "\n\n-----------------\nWriting errors to file\n-----------------\n"
  );
  // write parse erros to csv file
  // parse error is of the form {index: {sno, name, fathername, mobilenumber, reason}}

  let parseErrorCsv =
    "Block - " +
    BLOCK +
    "\n\n" +
    "S no,Name,Father / Husband Name,Mobile Number,Reason\n";

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

  fs.writeFileSync("./upload-out/" + BLOCK + "-errors.csv", parseErrorCsv);

  console.log("./upload-out/" + BLOCK + "-errors.csv\n\n");
}

async function makeCalls(records, retrying = false) {
  await createAll([...records]);

  console.log("\n\n-----------------\nSummary\n-----------------\n");
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
  return new Promise(async (resolve, reject) => {
    const promises = [];
    let i = 0;

    const farmerBuilds = [];
    console.log("\nBuilding farmers...");

    for (let j = 0; j < farmers.length; j++) {
      try {
        let farmer = buildFarmer(farmers[j], j);
        if (!farmer) continue;
        farmerBuilds.push(farmer);
      } catch (e) {
        console.log("Error building farmer - " + j);
        console.log("Farmer", farmers[j]);
        console.log("Error", e);
        console.log("Aborting upload.");
        process.exit(0);
      }
    }

    console.log("Complete.");
    console.log("Parse Errors: ", Object.keys(parseErrors).length);

    if (Object.keys(parseErrors).length > 0) {
      writeError();
    }

    // if (Object.keys(parseErrors).length > 20) {
    //   console.log("Too many parse errors. Aborting upload.");
    //   resolve();
    //   return;
    // }

    console.log(
      farmerBuilds.length,
      "built. \nBuild succeeded\n\nInitiating upload...\n-------------------------\n"
    );

    await getToken();

    if (!ACCESS_TOKEN) {
      console.log("Access token is required");
      process.exit(0);
    }

    sender = setInterval(() => {
      if (farmerBuilds.length === i) {
        clearInterval(sender);
        resolve(Promise.all(promises));
        return;
      }

      try {
        const data = farmerBuilds[i];

        try {
          console.log("Posting farmer", i);
          let promise = makeRequest(data, i);
          promises.push(promise);
        } catch (e) {
          console.log("Error", e, data);
        }

        i++;
      } catch (e) {
        console.log("Error", e, data);
      }
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
          if (res.body?.message === "Invalid user token") {
            console.log("Invalid access token or permission Denied");
            process.exit(0);
          } else if (res.statusCode !== 200) {
            failedIndexes[index] = {
              sno: index + 1,
              name: farmer.basicDetails.firstname.data,
              fathername: farmer.basicDetails.fatherorhusbandname.data,
              mobilenumber: farmer.basicDetails.mobilenumber.data,
              reason: body,
            };
            failed++;
          } else success++;
          return resolve();
        });
      });
      req.on("error", function (e) {
        throw e;
      });
      req.write(JSON.stringify(farmer));
      req.end();
    } catch (e) {
      failedIndexes[index] = {
        sno: index + 1,
        name: farmer?.basicDetails?.firstname?.data,
        fathername: farmer?.basicDetails?.fatherorhusbandname?.data,
        mobilenumber: farmer?.basicDetails?.mobilenumber?.data,
        reason: e?.message,
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
    if (farmer[4].trim().length !== 10 || isNaN(farmer[4].trim())) {
      throw new Error("Mobile number is not valid - " + farmer[4]);
    }

    // if farmer category is not small, marginal, ohers throw error
    if (
      !["small", "marginal", "others"].includes(farmer[5].toLowerCase().trim())
    ) {
      throw new Error("Farmer category is not valid - " + farmer[5]);
    }

    // convert BC to GEN
    if (farmer[6].toUpperCase().trim() === "BC") {
      farmer[6] = "GEN";
    }

    // if community is not SC, ST, MBC, GEN throw error
    if (!["SC", "ST", "MBC", "GEN"].includes(farmer[6].toUpperCase().trim())) {
      throw new Error("Farmer community is not valid");
    }

    // check if father name and name are alphabetic
    if (!/^[a-zA-Z \.]+$/.test(farmer[1].trim())) {
      throw new Error("Farmer name is not valid");
    }

    if (!/^[a-zA-Z \.]+$/.test(farmer[2].trim())) {
      throw new Error("Farmer father name is not valid");
    }

    // check if terrain area is of form 1.1 or 1
    if (!/^(?:[0-9]+\.)*[0-9]+$/.test(farmer[10].trim())) {
      throw new Error("Total extent area (Ha) is not valid - " + farmer[10]);
    }

    if (!/^(?:[0-9]+\.)*[0-9]+$/.test(farmer[11].trim())) {
      throw new Error("Cultivated area (Ha) is not valid - " + farmer[11]);
    }

    // if income is not a number throw error
    if (isNaN(farmer[18].trim())) {
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
          elementId: "state",
        },
        firstname: {
          label: "First Name",
          data:
            farmer[1].split(" ", 1)[0].toTitleCase().trim() ||
            farmer[1].trim().toTitleCase(),
          type: "text",
          elementId: "firstname",
        },
        lastname: {
          label: "Last Name",
          data: farmer[1].split(" ", 1)[1]?.toTitleCase().trim() || "",
          type: "text",
          elementId: "lastname",
        },
        mobilenumber: {
          label: "Mobile Number",
          data: farmer[4].trim(),
          type: "text",
          elementId: "mobilenumber",
        },
        yearlyincome: {
          label: "Yearly Income",
          data: { 2022: farmer[18].trim() },
          type: "map",

          elementId: "yearlyincome",
        },
        city: {
          label: "City",
          data: "Erode",
          type: "text",
          elementId: "city",
        },
        address: {
          label: "Area",
          data: farmer[3].trim(),
          type: "text",
          elementId: "address",
        },
        farmercategory: {
          label: "Farmer Category",
          data: farmer[5].trim().toTitleCase(),
          type: "text",
          elementId: "farmercategory",
        },
        fatherorhusbandname: {
          label: "Father or Husband Name",
          data: farmer[2].trim().toTitleCase(),
          type: "text",
          elementId: "fatherorhusbandname",
        },
        community: {
          label: "Community",
          data: farmer[6].trim(),
          type: "text",
          elementId: "community",
        },
      },
      farmingDetails: {
        cattleInformation: {
          label: "Cattle information",
          data: getCattles(farmer[16]),
          type: "map",
          elementId: "cattleInformation",
        },
        salesMethod: {
          data: getSales(farmer[17]),
          label: "Sales Method",
          type: "map",
          elementId: "salesMethod",
        },
        treesCultivation: {
          data: {},
          label: "Trees Cultivation",
          type: "map",
          elementId: "treesCultivation",
        },
      },
      landDetails: {
        cropscultivated: {
          label: "Crops cultivated",
          data: getCrops(farmer[12]),
          type: "list",
          elementId: "cropscultivated",
        },
        blocknumber: {
          label: "Block Number",
          data: BLOCK,
          type: "text",
          elementId: "blocknumber",
        },
        landownername: {
          label: "owner Name",
          data: getOwnerName(farmer[1], farmer[7]),
          type: "text",
          elementId: "landownername",
        },
        surveynumber: {
          label: "Survey Number",
          data: getSurveyNumber(farmer[8]),
          type: "text",
        },
        totalextentarea: {
          label: "Total extent area",
          data: farmer[10].trim(),
          type: "text",
          elementId: "totalextentarea",
        },
        "cultivatedarea(ha)": {
          label: "Cultivated area(Ha)",
          data: farmer[11].trim(),
          type: "text",
          elementId: "cultivatedarea(ha)",
        },
        sourcesofirrigation: {
          label: "Sources of Irrigation",
          data: getIrrigation(farmer[14]),
          type: "list",
        },
        electricity: {
          label: "Electricity",
          data: farmer[15].toTitleCase().trim() == "yes" ? "EB" : "",
          type: "text",
          electricity: "electricity",
        },
        landclassification: {
          label: "Land Classification",
          data: farmer[13].trim().toTitleCase(),
          type: "text",
          elementId: "landclassification",
        },
        SubdivisionNumber: {
          data: getSubDivisionNumber(farmer[9]),
          label: "Sub Division Numbers",
          type: "list",
          elementId: "SubdivisionNumber",
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
    if (!Farmer.basicDetails.firstname.data) {
      throw new Error("Farmer name is empty");
    }

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
    // throw e;
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
  return surveyNumbers;
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
      throw new Error("Cattle information is invalid : " + cattle);
    }

    if (parseInt(count) < 0) {
      return;
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
      const match = i.trim().match(/^(?:[0-9a-zA-Z]+)*[0-9a-zA-Z]+$/g);
      if (match) {
        return i.trim().toUpperCase();
      }
      throw new Error("Invalid sub division number " + i);
    });
}
