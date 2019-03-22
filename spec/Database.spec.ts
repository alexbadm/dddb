import { expect } from "chai";
import { Database } from "../src";

describe("Database class", () => {
  it("basic functions", () => {
    const db = new Database();
    db.defineHeader("Expenses", ["destination", "sum", "comment"]);

    expect(db.action(["Expenses", "meal", 600])).to.be.true;
    expect(db.action(["Expenses", "meal", 300, "eat out"])).to.be.true;
    expect(db.getTableInfo("Expenses")).to.be.deep.equal({
      header: [ "destination", "sum", "comment" ],
      types: [ "string", "number" ],
    });

    db.inferTypesFor("Expenses");
    expect(db.getTableInfo("Expenses")).to.be.deep.equal({
      header: [ "destination", "sum", "comment" ],
      types: [ "string", "number", "string" ],
    });
  });

  it("serialize/deserialize", () => {
    const db = new Database();
    expect(db.action(["Expenses", "meal", 600, "meat, bread, milk, etc."])).to.be.true;
    expect(db.action(["Expenses", "meal", 300, "eat out"])).to.be.true;
    db.defineHeader("Expenses", ["destination", "sum", "comment"]);

    const serialized = db.serialize();
    // console.log(serialized);
    expect(Database.deserialize(serialized)).to.be.deep.equal(db);
    // console.log(Database.deserialize(serialized));
  });
});
