import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { utils, writeFile, type WorkBook } from "xlsx";

type StoreRecord = {
  storeId: string;
  storeName: string;
  format: string;
  region: string;
  city: string;
  state: string;
  squareMeters: number;
  cluster: string;
};

type ItemRecord = {
  itemSku: string;
  itemDescription: string;
  department: string;
  category: string;
  subcategory: string;
  brand: string;
  packSize: string;
  unitOfMeasure: string;
  isPrivateLabel: "Yes" | "No";
  launchDate: string;
  discontinuedDate: string | null;
  baseVatRate: number;
  costExVat: number;
  shelfLifeDays: number | null;
  isAgeRestricted: "Yes" | "No";
  storageType: string;
  countryOfOrigin: string;
  supplierId: string;
  supplierName: string;
};

type PromotionRecord = {
  promotionId: string;
  promotionName: string;
  mechanic: string;
  fundedBy: string;
  startDate: string;
  endDate: string;
  discountPct: number;
  targetDepartment: string;
};

type TransactionLineRecord = {
  transactionId: string;
  basketId: string;
  businessDate: string;
  transactionTimestamp: string;
  storeId: string;
  posTerminalId: string;
  receiptNumber: string;
  lineNumber: number;
  cashierId: string;
  channel: string;
  paymentMethod: string;
  loyaltyTier: string;
  itemSku: string;
  itemDescription: string;
  department: string;
  category: string;
  brand: string;
  units: number;
  unitPriceInclVat: number;
  unitPriceExVat: number;
  lineDiscountInclVat: number;
  vatRate: number;
  netSalesExVat: number;
  vatAmount: number;
  grossSalesInclVat: number;
  cogsExVat: number;
  grossMarginExVat: number;
  returnFlag: "Yes" | "No";
};

const workbookPath = resolve(
  "apps/web/fixtures/demo-workbooks/retailer-transactions-demo.xlsx"
);
const profilePath = resolve(
  "apps/web/fixtures/demo-workbooks/retailer-transactions-demo.profile.json"
);

const storeSeedData = [
  [
    "ST001",
    "North Harbor Hyper",
    "Hypermarket",
    "North",
    "Seattle",
    "WA",
    "2018-05-14",
    5400,
    "Urban A",
    "No",
    "Yes",
  ],
  [
    "ST002",
    "Rainier Express",
    "Convenience",
    "North",
    "Tacoma",
    "WA",
    "2020-11-03",
    680,
    "Urban B",
    "Yes",
    "No",
  ],
  [
    "ST003",
    "Bridgeview Market",
    "Supermarket",
    "Midwest",
    "Chicago",
    "IL",
    "2016-08-22",
    2900,
    "Metro Core",
    "No",
    "Yes",
  ],
  [
    "ST004",
    "Oak Ridge Super",
    "Supermarket",
    "South",
    "Nashville",
    "TN",
    "2019-02-10",
    2500,
    "Suburban A",
    "No",
    "No",
  ],
  [
    "ST005",
    "Lakeside Family",
    "Supermarket",
    "Midwest",
    "Madison",
    "WI",
    "2017-09-01",
    2300,
    "College Town",
    "No",
    "Yes",
  ],
  [
    "ST006",
    "Sunset Neighborhood",
    "Convenience",
    "West",
    "San Diego",
    "CA",
    "2021-06-19",
    720,
    "Urban B",
    "No",
    "No",
  ],
  [
    "ST007",
    "Canyon Plaza Hyper",
    "Hypermarket",
    "West",
    "Phoenix",
    "AZ",
    "2015-03-28",
    6100,
    "Regional Hub",
    "Yes",
    "Yes",
  ],
  [
    "ST008",
    "Magnolia Fresh",
    "Supermarket",
    "South",
    "Atlanta",
    "GA",
    "2018-10-07",
    2800,
    "Metro Core",
    "No",
    "Yes",
  ],
  [
    "ST009",
    "Harbor Point Local",
    "Convenience",
    "Northeast",
    "Boston",
    "MA",
    "2022-01-15",
    640,
    "Urban A",
    "No",
    "No",
  ],
  [
    "ST010",
    "Liberty Central",
    "Supermarket",
    "Northeast",
    "Philadelphia",
    "PA",
    "2016-12-05",
    3100,
    "Metro Core",
    "No",
    "Yes",
  ],
  [
    "ST011",
    "Pinecrest Value",
    "Discount",
    "South",
    "Orlando",
    "FL",
    "2019-07-11",
    3500,
    "Suburban B",
    "No",
    "No",
  ],
  [
    "ST012",
    "Riverbend Wholesale",
    "Cash & Carry",
    "Midwest",
    "Columbus",
    "OH",
    "2014-04-17",
    7600,
    "Regional Hub",
    "Yes",
    "No",
  ],
] as const;

const categoryCatalog = [
  {
    department: "Grocery",
    category: "Beverages",
    subcategories: ["Sparkling Water", "Juice", "Soda", "Energy Drink"],
    brands: ["AquaVale", "Citrus Trail", "FizzUp", "Pulse8"],
    vatRate: 0.12,
    storageType: "Ambient",
    ageRestricted: false,
    uom: "EA",
  },
  {
    department: "Grocery",
    category: "Snacks",
    subcategories: ["Potato Chips", "Nuts", "Protein Bars", "Crackers"],
    brands: ["CrunchHouse", "Roam", "PeakFuel", "Salt Mill"],
    vatRate: 0.12,
    storageType: "Ambient",
    ageRestricted: false,
    uom: "EA",
  },
  {
    department: "Fresh",
    category: "Produce",
    subcategories: ["Bananas", "Apples", "Leafy Greens", "Tomatoes"],
    brands: ["FarmRoot", "Valley Picks", "Sun Basket", "Harvest Day"],
    vatRate: 0,
    storageType: "Chilled",
    ageRestricted: false,
    uom: "KG",
  },
  {
    department: "Fresh",
    category: "Dairy",
    subcategories: ["Milk", "Yogurt", "Butter", "Cheese"],
    brands: ["MeadowVale", "Daily Farm", "CreamLine", "Pasture Gold"],
    vatRate: 0.07,
    storageType: "Chilled",
    ageRestricted: false,
    uom: "EA",
  },
  {
    department: "Fresh",
    category: "Bakery",
    subcategories: ["Bread", "Croissant", "Muffin", "Bagel"],
    brands: ["Oven & Co", "Sunrise Bake", "Golden Crumb", "Morning Batch"],
    vatRate: 0.07,
    storageType: "Ambient",
    ageRestricted: false,
    uom: "EA",
  },
  {
    department: "Household",
    category: "Cleaning",
    subcategories: [
      "Detergent",
      "Surface Cleaner",
      "Dish Soap",
      "Paper Towels",
    ],
    brands: ["BrightNest", "PureHome", "Sparkly", "TidyPro"],
    vatRate: 0.25,
    storageType: "Ambient",
    ageRestricted: false,
    uom: "EA",
  },
  {
    department: "Health",
    category: "Personal Care",
    subcategories: ["Shampoo", "Soap", "Toothpaste", "Deodorant"],
    brands: ["FreshMark", "KindLab", "Dentora", "ClearDay"],
    vatRate: 0.25,
    storageType: "Ambient",
    ageRestricted: false,
    uom: "EA",
  },
  {
    department: "Pantry",
    category: "Staples",
    subcategories: ["Rice", "Pasta", "Flour", "Cooking Oil"],
    brands: ["Home Pantry", "Field Grain", "Cucina", "Golden Drop"],
    vatRate: 0.12,
    storageType: "Ambient",
    ageRestricted: false,
    uom: "EA",
  },
  {
    department: "Beverage",
    category: "Alcohol",
    subcategories: ["Beer", "Wine", "Cider", "Spirits"],
    brands: ["North Barrel", "Casa Verde", "Orchard Lane", "Stillhouse"],
    vatRate: 0.25,
    storageType: "Ambient",
    ageRestricted: true,
    uom: "EA",
  },
] as const;

const promotionTemplates = [
  [
    "PROMO-001",
    "Weekend Mix & Match",
    "Multi-buy",
    "Vendor",
    "2025-01-10",
    "2025-01-12",
    10,
    "Grocery",
  ],
  [
    "PROMO-002",
    "Healthy Start",
    "Percent Off",
    "Retailer",
    "2025-01-15",
    "2025-02-28",
    15,
    "Fresh",
  ],
  [
    "PROMO-003",
    "Big Game Beverage Push",
    "Percent Off",
    "Vendor",
    "2025-02-01",
    "2025-02-14",
    12,
    "Beverage",
  ],
  [
    "PROMO-004",
    "Spring Cleaning",
    "Percent Off",
    "Retailer",
    "2025-03-01",
    "2025-03-31",
    18,
    "Household",
  ],
  [
    "PROMO-005",
    "Loyalty Weekend",
    "Basket",
    "Retailer",
    "2025-03-14",
    "2025-03-16",
    8,
    "Grocery",
  ],
  [
    "PROMO-006",
    "Back to School Pantry",
    "Percent Off",
    "Vendor",
    "2025-08-05",
    "2025-08-25",
    10,
    "Pantry",
  ],
  [
    "PROMO-007",
    "Summer Hydration",
    "Percent Off",
    "Vendor",
    "2025-06-10",
    "2025-07-20",
    9,
    "Grocery",
  ],
  [
    "PROMO-008",
    "Holiday Entertaining",
    "Percent Off",
    "Retailer",
    "2025-11-20",
    "2025-12-24",
    14,
    "Beverage",
  ],
  [
    "PROMO-009",
    "Private Label Value",
    "Percent Off",
    "Retailer",
    "2025-04-01",
    "2025-12-31",
    6,
    "Grocery",
  ],
  [
    "PROMO-010",
    "Fresh Bakery Afternoon",
    "Markdown",
    "Retailer",
    "2025-01-01",
    "2025-12-31",
    20,
    "Fresh",
  ],
] as const;

const paymentMethods = ["Card", "Cash", "Mobile Wallet", "Gift Card"] as const;

const rng = createPrng(20260318);

const stores = buildStores();
const items = buildItems();
const promotions = buildPromotions();
const transactionLines = buildTransactionLines(stores, items, promotions);

const workbook: WorkBook = utils.book_new();

appendSheet(workbook, "Transactions", transactionLines);
appendSheet(
  workbook,
  "Items",
  items.map(({ department, ...item }) => {
    void department;
    return item;
  })
);
appendSheet(workbook, "Stores", stores);

mkdirSync(dirname(workbookPath), { recursive: true });
writeFile(workbook, workbookPath, { compression: true });
writeFileSync(
  profilePath,
  JSON.stringify(
    {
      workbookName: "retailer-transactions-demo.xlsx",
      generatedAt: new Date().toISOString(),
      sheets: [
        { name: "Transactions", rows: transactionLines.length },
        { name: "Items", rows: items.length },
        { name: "Stores", rows: stores.length },
      ],
      notes: [
        "Transactions are line-level receipt records spanning Q1 2025.",
        "Returns are represented as negative-unit lines with returnFlag=Yes.",
        "Prices include a mix of VAT rates (0%, 7%, 12%, 25%).",
        "Promotions join onto eligible lines via promotionId.",
        "The workbook is deterministic and can be regenerated from tools/generate-retail-demo-workbook.ts.",
      ],
    },
    null,
    2
  )
);

console.log(`Generated ${workbookPath}`);
console.log(`Generated ${profilePath}`);

function buildStores(): StoreRecord[] {
  return storeSeedData.map((entry) => ({
    storeId: entry[0],
    storeName: entry[1],
    format: entry[2],
    region: entry[3],
    city: entry[4],
    state: entry[5],
    squareMeters: entry[7],
    cluster: entry[8],
  }));
}

function buildItems(): ItemRecord[] {
  const itemsList: ItemRecord[] = [];
  let itemCounter = 1;

  for (const catalog of categoryCatalog) {
    for (const subcategory of catalog.subcategories) {
      for (let variantIndex = 0; variantIndex < 5; variantIndex += 1) {
        const brand = pick(catalog.brands);
        const size = buildPackSize(catalog.uom, subcategory, variantIndex);
        const baseCost = calculateBaseCost(
          catalog.department,
          catalog.category
        );
        const launchDate = dateString(
          2023 + (variantIndex % 3),
          1 + ((variantIndex * 2) % 12),
          5 + variantIndex
        );
        const isPrivateLabel = variantIndex % 4 === 0 ? "Yes" : "No";
        const supplierId = `SUP-${String(100 + itemCounter).padStart(4, "0")}`;
        const description = `${brand} ${subcategory} ${size}`;
        itemsList.push({
          itemSku: `SKU-${String(itemCounter).padStart(5, "0")}`,
          itemDescription: description,
          department: catalog.department,
          category: catalog.category,
          subcategory,
          brand,
          packSize: size,
          unitOfMeasure: catalog.uom,
          isPrivateLabel,
          launchDate,
          discontinuedDate: itemCounter % 37 === 0 ? "2025-09-30" : null,
          baseVatRate: catalog.vatRate,
          costExVat: roundCurrency(baseCost * (0.9 + rng() * 0.45)),
          shelfLifeDays:
            catalog.storageType === "Ambient"
              ? null
              : Math.floor(5 + rng() * 35),
          isAgeRestricted: catalog.ageRestricted ? "Yes" : "No",
          storageType: catalog.storageType,
          countryOfOrigin: pick([
            "USA",
            "Canada",
            "Mexico",
            "Spain",
            "Italy",
            "Netherlands",
          ]),
          supplierId,
          supplierName: `${pick(["Atlas", "Greenway", "Blue Peak", "Prime Source", "Northline"])} Supply`,
        });
        itemCounter += 1;
      }
    }
  }

  return itemsList;
}

function buildPromotions(): PromotionRecord[] {
  return promotionTemplates.map((entry) => ({
    promotionId: entry[0],
    promotionName: entry[1],
    mechanic: entry[2],
    fundedBy: entry[3],
    startDate: entry[4],
    endDate: entry[5],
    discountPct: entry[6],
    targetDepartment: entry[7],
  }));
}

function buildTransactionLines(
  storesList: StoreRecord[],
  itemsList: ItemRecord[],
  promotionsList: PromotionRecord[]
): TransactionLineRecord[] {
  const lines: TransactionLineRecord[] = [];
  const promotionByDepartment = new Map<string, PromotionRecord[]>();
  const itemsByDepartmentCategory = new Map<string, ItemRecord[]>();

  for (const promo of promotionsList) {
    const current = promotionByDepartment.get(promo.targetDepartment) ?? [];
    current.push(promo);
    promotionByDepartment.set(promo.targetDepartment, current);
  }

  for (const item of itemsList) {
    const key = `${item.department}::${item.category}`;
    const current = itemsByDepartmentCategory.get(key) ?? [];
    current.push(item);
    itemsByDepartmentCategory.set(key, current);
  }

  let transactionCounter = 1;

  for (let month = 1; month <= 3; month += 1) {
    const daysInMonth = new Date(Date.UTC(2025, month, 0)).getUTCDate();
    for (let day = 1; day <= daysInMonth; day += 1) {
      const businessDate = dateString(2025, month, day);
      for (const store of storesList) {
        const basketsToday = basketCountForDay(store, month, day);
        for (
          let basketIndex = 0;
          basketIndex < basketsToday;
          basketIndex += 1
        ) {
          const basketId = `BASK-${String(transactionCounter).padStart(7, "0")}`;
          const transactionId = `TX-${String(transactionCounter).padStart(8, "0")}`;
          const receiptNumber = `${store.storeId.replace("ST", "")}-${String(month).padStart(2, "0")}${String(day).padStart(2, "0")}-${String(basketIndex + 1).padStart(4, "0")}`;
          const lineCount = weightedPick([
            [1, 0.18],
            [2, 0.24],
            [3, 0.2],
            [4, 0.16],
            [5, 0.11],
            [6, 0.06],
            [7, 0.03],
            [8, 0.02],
          ]);
          const basketChannel = channelForStore(store);
          const paymentMethod = pick(paymentMethods);
          const loyaltyTier = weightedPick([
            ["None", 0.28],
            ["Bronze", 0.32],
            ["Silver", 0.24],
            ["Gold", 0.16],
          ]);
          const basketTimestamp = buildTimestamp(
            businessDate,
            store.format,
            basketIndex
          );

          for (let lineNumber = 1; lineNumber <= lineCount; lineNumber += 1) {
            const item = pickWeightedItem(itemsByDepartmentCategory);
            const units =
              item.unitOfMeasure === "KG"
                ? roundQuantity(0.4 + rng() * 1.8)
                : weightedPick([
                    [1, 0.82],
                    [2, 0.13],
                    [3, 0.04],
                    [4, 0.01],
                  ]);
            const priceExVat = roundCurrency(
              item.costExVat * (1.22 + rng() * 0.62)
            );
            const priceInclVat = roundCurrency(
              priceExVat * (1 + item.baseVatRate)
            );
            const applicablePromos =
              promotionByDepartment.get(item.department) ?? [];
            const promo =
              applicablePromos.length > 0 && rng() < 0.14
                ? pick(applicablePromos)
                : null;
            const discountInclVat = promo
              ? roundCurrency(
                  priceInclVat * Number(units) * (promo.discountPct / 100)
                )
              : roundCurrency(
                  priceInclVat * Number(units) * (rng() < 0.08 ? 0.05 : 0)
                );
            const isReturn = rng() < 0.012;
            const signedUnits = isReturn ? multiplyNumber(units, -1) : units;
            const grossSalesInclVat = roundCurrency(
              priceInclVat * Number(signedUnits) -
                (isReturn ? 0 : discountInclVat)
            );
            const netSalesExVat = roundCurrency(
              grossSalesInclVat / (1 + item.baseVatRate)
            );
            const vatAmount = roundCurrency(grossSalesInclVat - netSalesExVat);
            const cogsExVat = roundCurrency(
              item.costExVat * Number(signedUnits)
            );

            lines.push({
              transactionId,
              basketId,
              businessDate,
              transactionTimestamp: basketTimestamp,
              storeId: store.storeId,
              posTerminalId: `${store.storeId}-POS-${String(1 + (basketIndex % 8)).padStart(2, "0")}`,
              receiptNumber,
              lineNumber,
              cashierId: `${store.storeId}-C${String(1 + (basketIndex % 18)).padStart(3, "0")}`,
              channel: basketChannel,
              paymentMethod,
              loyaltyTier,
              itemSku: item.itemSku,
              itemDescription: item.itemDescription,
              department: item.department,
              category: item.category,
              brand: item.brand,
              units: Number(signedUnits),
              unitPriceInclVat: priceInclVat,
              unitPriceExVat: priceExVat,
              lineDiscountInclVat: isReturn ? 0 : discountInclVat,
              vatRate: item.baseVatRate,
              netSalesExVat,
              vatAmount,
              grossSalesInclVat,
              cogsExVat,
              grossMarginExVat: roundCurrency(netSalesExVat - cogsExVat),
              returnFlag: isReturn ? "Yes" : "No",
            });
          }

          transactionCounter += 1;
        }
      }
    }
  }

  return lines;
}

function appendSheet(
  workbookFile: WorkBook,
  name: string,
  rows: Record<string, unknown>[]
) {
  const sheet = utils.json_to_sheet(rows);
  utils.book_append_sheet(workbookFile, sheet, name);
}

function basketCountForDay(
  store: StoreRecord,
  month: number,
  day: number
): number {
  const baseByFormat: Record<string, number> = {
    Hypermarket: 9,
    Supermarket: 7,
    Convenience: 4,
    Discount: 8,
    "Cash & Carry": 6,
  };
  const seasonalMultiplier =
    month === 11 || month === 12 ? 1.22 : month >= 6 && month <= 8 ? 1.08 : 1;
  const weekendBoost = dayOfWeek(2025, month, day) >= 5 ? 1.16 : 1;
  const storeFactor = 0.84 + (numericSuffix(store.storeId) % 7) * 0.05;
  return Math.max(
    8,
    Math.round(
      (baseByFormat[store.format] ?? 12) *
        seasonalMultiplier *
        weekendBoost *
        storeFactor
    )
  );
}

function pickWeightedItem(
  itemsByDepartmentCategory: Map<string, ItemRecord[]>
): ItemRecord {
  const index = weightedPick([
    [0, 0.18],
    [1, 0.16],
    [2, 0.14],
    [3, 0.12],
    [4, 0.1],
    [5, 0.09],
    [6, 0.08],
    [7, 0.07],
    [8, 0.06],
  ]);
  const byCategory = categoryCatalog[index] ?? categoryCatalog[0];
  const filtered =
    itemsByDepartmentCategory.get(
      `${byCategory.department}::${byCategory.category}`
    ) ?? [];
  return pick(filtered);
}

function channelForStore(store: StoreRecord): string {
  if (store.format === "Convenience") {
    return weightedPick([
      ["In-Store", 0.92],
      ["Click & Collect", 0.05],
      ["Delivery", 0.03],
    ]);
  }

  if (store.format === "Cash & Carry") {
    return weightedPick([
      ["In-Store", 0.87],
      ["Click & Collect", 0.09],
      ["Delivery", 0.04],
    ]);
  }

  return weightedPick([
    ["In-Store", 0.82],
    ["Click & Collect", 0.1],
    ["Delivery", 0.08],
  ]);
}

function buildTimestamp(
  businessDate: string,
  format: string,
  basketIndex: number
): string {
  const hourBase =
    format === "Convenience" ? 6 : format === "Cash & Carry" ? 7 : 8;
  const minuteOfDay = Math.floor((basketIndex * 11 + rng() * 90) % 840);
  const hour = hourBase + Math.floor(minuteOfDay / 60);
  const minute = Math.floor(minuteOfDay % 60);
  const second = Math.floor(rng() * 60);
  return `${businessDate} ${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:${String(second).padStart(2, "0")}`;
}

function buildPackSize(
  uom: string,
  subcategory: string,
  variantIndex: number
): string {
  if (uom === "KG") {
    return `${roundQuantity(0.5 + variantIndex * 0.25)} kg`;
  }

  if (
    subcategory === "Beer" ||
    subcategory === "Soda" ||
    subcategory === "Sparkling Water"
  ) {
    return pick(["330 ml", "500 ml", "6 x 330 ml", "1.5 L"]);
  }

  return pick(["1 pack", "2 pack", "500 g", "750 g", "1 L"]);
}

function calculateBaseCost(department: string, category: string): number {
  const baseline: Record<string, number> = {
    Grocery: 1.2,
    Fresh: 1.8,
    Household: 3.6,
    Health: 2.9,
    Pantry: 1.7,
    Beverage: 4.8,
  };
  const categoryLift =
    category === "Alcohol" ? 2.4 : category === "Cleaning" ? 1.1 : 1;
  return (baseline[department] ?? 1.5) * categoryLift;
}

function createPrng(seed: number): () => number {
  let current = seed >>> 0;
  return () => {
    current = (1664525 * current + 1013904223) >>> 0;
    return current / 4294967296;
  };
}

function pick<T>(values: readonly T[]): T {
  return values[Math.floor(rng() * values.length)] as T;
}

function weightedPick<T>(pairs: ReadonlyArray<readonly [T, number]>): T {
  const threshold = rng();
  let cumulative = 0;
  for (const [value, weight] of pairs) {
    cumulative += weight;
    if (threshold <= cumulative) {
      return value;
    }
  }
  return pairs[pairs.length - 1]?.[0] as T;
}

function roundCurrency(value: number): number {
  return Math.round(value * 100) / 100;
}

function roundQuantity(value: number): number {
  return Math.round(value * 1000) / 1000;
}

function multiplyNumber(value: number, factor: number): number {
  return Math.round(value * factor * 1000) / 1000;
}

function numericSuffix(value: string): number {
  return Number.parseInt(value.replace(/\D/g, ""), 10);
}

function dateString(year: number, month: number, day: number): string {
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function dayOfWeek(year: number, month: number, day: number): number {
  return new Date(Date.UTC(year, month - 1, day)).getUTCDay();
}
