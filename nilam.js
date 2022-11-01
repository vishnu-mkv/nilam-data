const { parse } = require("csv-parse");
const fs = require("fs");
const http = require("http");

const BLOCK = "";
const FILE_PATH = "";
const ACCESS_TOKEN = "";

const records = [];
let success = 0,
  failed = 0;
const failedIndexes = {};
const parseErrors = {};

// Initialize the parser
const parser = parse({});
// Use the readable stream api to consume records
parser.on("readable", function () {
  let record;
  while ((record = parser.read()) !== null) {
    records.push(record);
  }
});

parser.on("end", function () {
  console.log("Waiting for all requests to complete");
  makeCalls(records);
  // process.exit(0);
});

function makeCalls(records, retrying = false) {
  if (retrying) console.log("Retrying");

  createAll([...records]).then(() => {
    console.log("Success", success);
    console.log("Failed", failed);
    console.log("Failed indexes", failedIndexes);
    console.log("Parse errors", parseErrors);
    console.log("retry", Object.keys(failedIndexes));

    const retry = Object.keys(failedIndexes).map((i) => records[i]);
    (failed = 0), (failedIndexes = {});
    if (retry.length > 0) {
      makeCalls(retry, true);
    }
  });
}

// takes array of farmers and sends requests to create farmers with a delay of 100ms between each request and then resolves the promise
// when all requests are complete

function createAll(farmers) {
  return new Promise((resolve, reject) => {
    const promises = [];
    let i = 0;

    const sender = setInterval(() => {
      if (farmers.length === i) {
        clearInterval(sender);
        resolve(Promise.all(promises));
        return;
      }

      try {
        const data = farmers[i];
        let farmer = buildFarmer(data, i);

        console.log("Posting farmer", i);
        let promise = makeRequest(farmer, i);
        i++;
        promises.push(promise);
      } catch (e) {}
    }, 300);
  });
}

function makeRequest(farmer, index) {
  var options = {
    host: "localhost",
    port: 5000,
    path: "/nilam-a01df/asia-south1/widgets/report/create-farmer",
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
          if (res.statusCode !== 200) {
            failedIndexes[index] =
              body + " " + farmer.basicDetails.firstname.data;
            failed++;
          } else success++;
          return resolve();
        });
      });
      req.on("error", function (e) {
        failedIndexes[index] = e + " " + farmer.basicDetails.firstname.data;
        failed++;
        return resolve();
      });
      req.write(JSON.stringify(farmer));
      req.end();
    } catch (e) {
      failedIndexes[index] = e + " " + farmer.basicDetails.firstname.data;
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
    if (farmer[7].toUpperCase() === "AI" || farmer[7].toUpperCase() === "A1") {
      //  ascii ` is 60, a is 61
      farmer[7] = "`";
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
          data: farmer[1].split(" ")[0],
          type: "text",
        },
        lastname: {
          label: "Last Name",
          data: farmer[1].split(" ")[1] || "",
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
        area: {
          label: "Area",
          data: farmer[3],
          type: "text",
        },
        farmertype: {
          label: "Farmer Type",
          data: farmer[5],
          type: "text",
        },
        fathername: {
          label: "Father Name",
          data: farmer[2],
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
          data: farmer[8],
          type: "text",
        },
        totalterrianarea: {
          label: "Total Terrian Area",
          data: farmer[10],
          type: "text",
        },
        terrianareaundercultivation: {
          label: "Terrian Area under Cultivation",
          data: farmer[11],
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
          data: farmer[13],
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
    console.log("Error building " + index, e);
    parseErrors[i] = e + " " + farmer.basicDetails.firstname.data;
  }
}

function getCattles(cattle) {
  if (cattle === "" || cattle === "-" || cattle.toLowerCase() === "nil")
    return {};
  const ret = {};
  const cattles = cattle.split(",");
  cattles.forEach((cattle) => {
    const [name, count] = cattle.split("-");
    ret[name.trim()] = parseInt(count);
  });
  return ret;
}

function getCrops(crops) {
  if (crops === "" || crops === "-" || crops.toLowerCase() === "nil") return [];
  return crops.split(",");
}

function getSales(sales) {
  if (sales === "" || sales === "-" || sales.toLowerCase() === "nil") return {};
  const ret = {};
  const salesArr = sales.split(",");
  salesArr.forEach((sale) => {
    ret[sale.trim()] = "";
  });
  return ret;
}

function getOwnerName(name, class_) {
  if (class_ !== "D") return name;
  return "";
}

function getIrrigation(irrigation) {
  if (
    irrigation === "" ||
    irrigation === "-" ||
    irrigation.toLowerCase() === "nil"
  )
    return [];
  return irrigation.split(",");
}

function getSubDivisionNumber(subDiv) {
  if (subDiv === "" || subDiv === "-" || subDiv.toLowerCase() === "nil")
    return [];
  return subDiv.split(",");
}
