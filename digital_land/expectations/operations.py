# # dataset,entity,field,operation,value,notes
# # conservation-area,44000001,name,matches,Napsbury,
# # conservation-area,44000001,organisation-entity,matches,278,
# # conservation-area,44000001,reference,matches,CA18,
# # conservation-area,44009844,name,matches,AUSTIN VILLAGE CONSERVATION AREA,
# # conservation-area,44009844,organisation-entity,matches,44,
# # conservation-area,44009844,reference,matches,30,
# # conservation-area,44002411,name,matches,Railway Terraces,
# # conservation-area,44002411,organisation-entity,matches,48,
# # conservation-area,44002411,reference,matches,CA11,

# #
# from api import API

# # def matches(conn,entity:int,field:str,value):
# #     """
# #     looks at the entity table to check if the fielld and value matches what is expected

# #     Args
# #         conn: a sqlite connection to the db to be used for querying
# #         entity: an integer value representing the entity
# #         field: a string representing the field to check the value of
# #         value: could be any number of types depending ont he field
# #     """
# #     print("Expect entity: ", expect["entity"])
# #     field = expect["field"].replace("-", "_")
# #     cursor = connection.cursor()
# #     query = f"select {field} from entity" + (
# #         f" where entity = '{expect['entity']}'" if expect["entity"] else ""
# #     )
# #     print(query)
# #     cursor.execute(query)
# #     rows = cursor.fetchall()

# #     if len(rows) < 1:
# #         print(f"{expect['dataset']} missing entity {expect['entity']}", file=sys.stderr)
# #         errors += 1

# # TODO is there a way to represent this in a generalised count or not
# def count_lpa_boundary(conn,lpa,expected,geometric_relation='within'):
#     """
#     Specific version of a count which given a local authority
#     and a dataset checks for  any entities relating to the lpa boundary.
#     relation defaults to within but can be changed.
#     """
#     # get lpa boundary
#     ## get lpa boundary code from org
#     API.get('entity',params={'field':['local-planning-authority','entity'],'curie':lpa})

#     ## get geometric boundary from API
#     API.get('entity',params={'field':'geomtry','curie':{lpa_boundary_curie}})

#     # do a spatial query on the sqlite
#     query = """
#         SELECT entity
#         FROM entity
#         WHERE geometry != ''
#         AND organisation = '{lpa}'
#         AND ST_WITHIN(ST_GEOMFROMWKT(geometry),ST_GEOMFROMWKT({lpa_boundary})
#     """

#     rows = conn.execute(query).fetchall()
#     entities = [row[0] for row in rows]
#     # compare expected to actual

#     result = actual == expected

#     # if result:
#     #     # There were the expected number of entities

#     # details = {
#     #     'actual': actual,
#     #     'expected': expected,
#     #     'entities':entities,
#     # }
