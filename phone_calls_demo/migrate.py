from grakn.client import GraknClient

inputs = [
    {

    "data_path": "data/companies",
    "template": company_template

    },
    {

    "data_path": "data/people",
    "template": people_template

    },
    {

    "data_path": "data/contracts",
    "template": contract_template

    },
    {

    "data_path": "data/calls",
    "template": call_template

    }
]

def build_phone_call_graph(inputs):
    with GraknClient(uri="localhost:48555") as client:
        with client.session(keyspace = "phone_calls") as session:
            for input in inputs:
                print("Loading from [" + input["data_path"] + "] into Grakn ...")
                load_data_into_grakn(input, session)

def load_data_into_grakn(input, session):
    items = parse_data_to_dictionaries(input)

    for item in items:
        with session.transaction().write() as transaction:
            graql_insert_query = input["template"](item)
            print("Executing Graql Query: " + graql_insert_query)
            transaction.query(graql_insert_query)
            transaction.commit()

    print("\nInserted " + str(len(items)) + " items from [" + input["data_path"] + "] into Grakn.\n")

def company_template(company):
    return 'insert $company isa company, has name "' + company["name"] + '";'

def person_template(person):
    #insert person
    graql_insert_query = 'insert $person isa person, has phone-number "' + person["phone_number"] + '"'
    if "first_name" in person:

        #person is a customer
        graql_insert_query += ", has is-customer true"
        graql_insert_query += ', has first-name"' + person["first_name"] + "'"
        graql_insert_query += ', has last-name "' + person["last_name"] + "'"
        graql_insert_query +=
        graql_insert_query +=


build_phone_call_graph(inputs)
