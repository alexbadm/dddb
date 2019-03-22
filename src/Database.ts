
type TAction = TField[];

interface IDatabase {
  [spaceName: string]: ISpace;
}

interface ISpace {
  [tableName: string]: ITable;
}

interface ITable<T = TField[]> {
  header: string[];
  minTupleLen: number;
  types?: FieldType[];
  values: T[];
}

interface ITableDescriptor {
  header: string[];
  minTupleLen: number;
}

interface ITableDescriptors {
  [spaceAndTableName: string]: ITableDescriptor;
}

interface ITable<T = TField[]> extends ITableDescriptor {
  types?: FieldType[];
  values: T[];
}

// enum FieldType {
//   string,
//   number,
//   boolean,
// }
type FieldType = "string" | "number" | "bigint" | "boolean" | "symbol" | "undefined" | "object" | "function";
type TField = string | number | bigint | boolean | symbol | undefined | object;

export class Database {
  public static deserialize(source: string): Database {
    const { actions, tableDescriptors } = JSON.parse(source);
    if (!actions || !tableDescriptors) {
      throw new Error("Failed to deserialize. Wrong data format.");
    }
    const db = new Database(actions);
    db.setTableDescriptors(tableDescriptors);
    return db;
  }

  public static serialize(db: Database): string {
    return JSON.stringify({
      actions: db.dbActions,
      tableDescriptors: db.getTableDescriptors(),
    });
  }

  public db: IDatabase = { default: {} };
  protected dbActions: { [space: string]: TAction[] } = {};

  constructor(dbActions?: { [space: string]: TAction[] }) {
    if (dbActions) {
      Object.entries(dbActions).forEach(([space, actionLog]) => {
        this.restoreDBFromLog(actionLog, space);
      });
    }
  }

  public action(act: TAction, spaceName = "default"): boolean {
    const tableName = act[0];
    if (typeof tableName !== "string") {
      return false;
    }
    this.assertTable(tableName, spaceName);
    this.dbActions[spaceName].push(act);

    const tuple = act.slice(1);
    const table = this.db[spaceName][tableName];
    if (!table.types) {
      table.types = inferTypesFrom(tuple);
    }

    if (!validateTuple(tuple, { minTupleLen: table.minTupleLen, types: table.types })) {
      // throw new Error("Validation Error: the tuple is of the wrong type");
      return false;
    }
    table.values.push(tuple);
    return true;
  }

  public defineHeader(tableName: string, header: string[], spaceName = "default") {
    this.assertTable(tableName, spaceName);
    this.db[spaceName][tableName].header = header;
  }

  public getTableDescriptors(): ITableDescriptors {
    const descriptors: ITableDescriptors = {};
    Object.keys(this.db).forEach((spaceName) => {
      Object.keys(this.db[spaceName]).forEach((tableName) => {
        const { header, minTupleLen } = this.db[spaceName][tableName];
        descriptors[`${spaceName}:${tableName}`] = { header, minTupleLen };
      });
    });
    return descriptors;
  }

  public getTableInfo(tableName: string, spaceName = "default"): { header: string[]; types?: FieldType[]; } {
    this.assertTable(tableName, spaceName);
    const { header, types } = this.db[spaceName][tableName];
    return {
      header: header.slice(),
      types: types && types.slice(),
    };
  }

  public inferTypesFor(tableName: string, spaceName = "default"): FieldType[] {
    const table = this.db[spaceName] && this.db[spaceName][tableName];
    if (!table) {
      return [];
    }

    const maxLength = Math.max.apply(undefined, table.values.map((value) => value.length));
    const fullestValue = table.values.find((v) => v.length === maxLength);
    if (!fullestValue) {
      return [];
    }
    return table.types = inferTypesFrom(fullestValue);
  }

  public serialize(): string {
    return Database.serialize(this);
  }

  public setTableDescriptors(descriptors: ITableDescriptors): void {
    Object.keys(this.db).forEach((spaceName) => {
      Object.entries(this.db[spaceName]).forEach(([tableName, table]) => {
        const { header, minTupleLen } = descriptors[`${spaceName}:${tableName}`];
        table.header = header;
        table.minTupleLen = minTupleLen;
      });
    });
  }

  protected assertTable(tableName: string, spaceName: string): void {
    this.assertSpace(spaceName);
    if (!(tableName in this.db[spaceName])) {
      this.db[spaceName][tableName] = { header: [], minTupleLen: 0, values: [] };
    }
  }

  protected assertSpace(spaceName: string): void {
    if (!(spaceName in this.dbActions)) {
      this.dbActions[spaceName] = [];
    }
    if (!(spaceName in this.db)) {
      this.db[spaceName] = {};
    }
  }

  protected restoreDBFromLog(dbActions?: TAction[], spaceName = "default") {
    if (dbActions) {
      this.assertSpace(spaceName);
      const space = this.db[spaceName];

      for (const act of dbActions) {
        const tableName = act[0] as string;
        if (!(tableName in space)) {
          space[tableName] = { header: [], minTupleLen: 0, values: [] };
        }
        this.dbActions[spaceName].push(act);
        space[tableName].values.push(act.slice(1));
      }

      Object.keys(space).forEach((tableName) => {
        this.inferTypesFor(tableName, spaceName);
      });
    }
  }
}

function inferTypesFrom(tuple: TField[]): FieldType[] {
  return tuple.map((value) => typeof value);
}

function validateTuple(
  tuple: TField[],
  { minTupleLen, types }: { minTupleLen: number; types: FieldType[]; },
): boolean {
  if (tuple.length < minTupleLen) {
    return false;
  }
  const count = Math.min(tuple.length, types.length);
  for (let i = 0; i < count; i++) {
    if (types[i] !== typeof tuple[i]) {
      return false;
    }
  }
  return true;
}
