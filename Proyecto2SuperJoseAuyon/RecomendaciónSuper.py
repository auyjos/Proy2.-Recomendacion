# Autor: José Auyón 201579
# Mayo 31, 2022

from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable


class Recommender:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):

        self.driver.close()

    def encuentra_relacion(self, nombre_articulo):
        with self.driver.session() as session:
            result = session.read_transaction(self.encuentra_articulo, nombre_articulo)
            return {"friends": result}

    @staticmethod
    def encuentra_articulo(tx, nombre_articulo):
        query = (

            "MATCH (p:Person {name: $nombre_articulo})-[:PERTENECE]-(producto) "
            "RETURN producto.name AS name"
        )
        result = tx.run(query,nombre_articulo=nombre_articulo)
        return [record["producto"] for record in result]

    # get recommendations from the database, to be called by our FastAPI server
    def find_recommendation(self, tipo_producto, ubicacion, lista_persona, max):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_recommendation, tipo_producto, ubicacion,
                                              lista_persona, max)
            return result

    @staticmethod
    def _find_and_return_recommendation(tx, tipo_producto, ubicacion, lista_persona, max):

        tipo_producto_string = "(producto)" if tipo_producto == '' else "(tipo:Producto {name: $tipo_producto})"
        ubicacion_string = "(ubicacion)" if ubicacion == ''  else "(ubicacion:Ubicacion {name: $ubicacion})"
        lista_persona_string = "" if len(lista_persona) == 0 else "WHERE person.name IN %s" % (str(lista_persona))


        if (max):
            query = (
                '''MATCH (super:Super)-[:LOCATED_IN]->{ubicacion},
                      (super)-[:TIENE]->{tipo},
                      (person:Person)-[:GUSTA]->(tipo)
                {person}
                WITH restaurant.name AS name, collect(super.name) AS likers, COUNT(*) AS occurence
                WITH MAX(occurence) as max_count
                MATCH (super:Super)-[:UBICADO]->{ubicacion},
                      (super)-[:TIENE]->{tipo},
                      (person:Person)-[:LIKES]->(super)
                {person}
                WITH super AS name, collect(super.name) AS likers, COUNT(*) AS occurence, max_count
                WHERE ocurrencia = max_count
                RETURN name, likers, ocurrencia '''.format(ubicacion=ubicacion_string, tipo_producto= tipo_producto_string,
                                                        lista_persona=lista_persona_string)
            )
        else:
            query = (
                    '''MATCH (super:Super)-[:LOCATED_IN]->%s,
                          (super)-[:TIENE]->%s,
                          (person:Person)-[:GUSTA]->(super)
                    %s
                    RETURN super.name AS name, collect(supern.name) AS likers, COUNT(*) AS ocurrencia
                    ORDER BY ocurrencia DESC''' % (ubicacion_string, tipo_producto_string, lista_persona_string)
            )

        result = tx.run(query, tipo_producto=tipo_producto, ubicacion=ubicacion, lista_persona=lista_persona)
        try:
            return [{"super": row["name"], "likers": row["likers"], "occurence": row["occurence"]} for row in
                    result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise



if __name__ == "__main__":
    uri = "neo4j+s://a0f475d5.databases.neo4j.io:7687"
    user = "neo4j"
    password = "OmzKwjDEklHMSuJyV9dBjri69lfuB34fb8i8XOYHiuU"
    app = Recommender(uri, user, password)

    app.close()