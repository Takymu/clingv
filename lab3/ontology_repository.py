from neo4j import GraphDatabase
import random
import string
from typing import Optional, List, Dict, Any

class Neo4jOntologyRepository:
    def __init__(self, uri: str, user: str, password: str, namespace: str = "http://ontology-lab.com/"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.namespace = namespace

    def close(self):
        self.driver.close()

    def generate_random_string(self, length: int = 8) -> str:
        letters = string.ascii_letters + string.digits
        return f"{self.namespace}{''.join(random.choice(letters) for _ in range(length))}"

    def _collect_node(self, node) -> Optional[Dict[str, Any]]:
        if not node:
            return None
        props = dict(node)
        labels = list(node.labels)
        return {
            "uri": props.get("uri", ""),
            "title": props.get("title", props.get("description", "")),
            "description": props.get("description", ""),
            "label": labels[0] if labels else "Unknown",
            "props": {k: v for k, v in props.items() if k not in ["uri", "description", "title"]}
        }

    def _collect_arc(self, relationship, node_from_uri: str, node_to_uri: str) -> Optional[Dict[str, Any]]:
        if not relationship:
            return None
        return {
            "id": relationship.element_id,
            "uri": relationship.type,
            "node_uri_from": node_from_uri,
            "node_uri_to": node_to_uri
        }

    def get_all_nodes_and_arcs(self) -> Dict[str, List]:
        query = "MATCH (n) OPTIONAL MATCH (n)-[r]->(m) RETURN n, r, m"
        result_data = {"nodes": {}, "arcs": []}
        with self.driver.session() as session:
            result = session.run(query)
            for record in result:
                node_obj = self._collect_node(record["n"])
                if not node_obj:
                    continue
                if node_obj["uri"] not in result_data["nodes"]:
                    result_data["nodes"][node_obj["uri"]] = node_obj
                rel, target = record["r"], record["m"]
                if rel and target and target.get("uri"):
                    result_data["arcs"].append(self._collect_arc(rel, node_obj["uri"], target.get("uri")))
        return {"nodes": list(result_data["nodes"].values()), "arcs": result_data["arcs"]}

    def get_nodes_by_labels(self, labels: List[str]) -> List[Dict]:
        labels_str = "".join([f":`{l}`" for l in labels])
        with self.driver.session() as session:
            return [self._collect_node(r["n"]) for r in session.run(f"MATCH (n{labels_str}) RETURN n")]

    def delete_node_by_uri(self, uri: str):
        with self.driver.session() as session:
            session.run("MATCH (n) WHERE n.uri = $uri DETACH DELETE n", uri=uri)

    def delete_arc_by_id(self, arc_id: str):
        with self.driver.session() as session:
            try:
                session.run("MATCH ()-[r]-() WHERE elementId(r) = $id DELETE r", id=str(arc_id))
            except:
                session.run("MATCH ()-[r]-() WHERE id(r) = $id DELETE r", id=int(arc_id))

    def clear_database(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

    def run_custom_query(self, query: str, params: Dict = None) -> List[Dict]:
        with self.driver.session() as session:
            return [dict(r) for r in session.run(query, params or {})]

    def get_ontology(self) -> Dict[str, Any]:
        result = {"classes": [], "objects": [], "datatype_properties": [], "object_properties": [], "arcs": []}
        with self.driver.session() as session:
            class_uris, object_uris = set(), set()
            for r in session.run("MATCH (c:Class) OPTIONAL MATCH (c)-[rel]->(t) RETURN c, rel, t"):
                c = self._collect_node(r["c"])
                if c and c["uri"] not in class_uris:
                    result["classes"].append(c)
                    class_uris.add(c["uri"])
                if r["rel"] and r["t"] and r["t"].get("uri"):
                    result["arcs"].append(self._collect_arc(r["rel"], c["uri"], r["t"].get("uri")))
            for r in session.run("MATCH (o:Object) OPTIONAL MATCH (o)-[rel]->(t) WHERE NOT type(rel) = 'rdf_type' RETURN o, rel, t"):
                o = self._collect_node(r["o"])
                if o and o["uri"] not in object_uris:
                    result["objects"].append(o)
                    object_uris.add(o["uri"])
                if r["rel"] and r["t"] and r["t"].get("uri"):
                    result["arcs"].append(self._collect_arc(r["rel"], o["uri"], r["t"].get("uri")))
            for r in session.run("MATCH (d:DatatypeProperty) RETURN d"):
                result["datatype_properties"].append(self._collect_node(r["d"]))
            for r in session.run("MATCH (o:ObjectProperty) RETURN o"):
                result["object_properties"].append(self._collect_node(r["o"]))
        return result

    def get_ontology_parent_classes(self) -> List[Dict]:
        with self.driver.session() as session:
            return [self._collect_node(r["c"]) for r in session.run("MATCH (c:Class) WHERE NOT (c)-[:rdfs_subClassOf]->(:Class) RETURN c")]

    def get_class(self, class_uri: str) -> Optional[Dict]:
        query = """
        MATCH (c:Class {uri: $uri})
        OPTIONAL MATCH (c)-[:rdfs_subClassOf]->(parent:Class)
        OPTIONAL MATCH (c)-[:domain]->(dp:DatatypeProperty)
        OPTIONAL MATCH (c)-[:domain]->(op:ObjectProperty)
        RETURN c, parent.uri as parent_uri, collect(DISTINCT dp.uri) as dp_uris, collect(DISTINCT op.uri) as op_uris
        """
        with self.driver.session() as session:
            r = session.run(query, uri=class_uri).single()
            if r:
                obj = self._collect_node(r["c"])
                obj.update({"parent_uri": r["parent_uri"], "datatype_properties": [p for p in r["dp_uris"] if p], "object_properties": [p for p in r["op_uris"] if p]})
                return obj
        return None

    def get_class_parents(self, class_uri: str) -> List[Dict]:
        with self.driver.session() as session:
            return [self._collect_node(r["parent"]) for r in session.run("MATCH (c:Class {uri: $uri})-[:rdfs_subClassOf*]->(parent:Class) RETURN DISTINCT parent", uri=class_uri)]

    def get_class_children(self, class_uri: str) -> List[Dict]:
        with self.driver.session() as session:
            return [self._collect_node(r["child"]) for r in session.run("MATCH (child:Class)-[:rdfs_subClassOf*]->(c:Class {uri: $uri}) RETURN DISTINCT child", uri=class_uri)]

    def get_class_objects(self, class_uri: str) -> List[Dict]:
        with self.driver.session() as session:
            return [self._collect_node(r["o"]) for r in session.run("MATCH (o:Object)-[:rdf_type]->(c:Class {uri: $uri}) RETURN o", uri=class_uri)]

    def update_class(self, class_uri: str, title: str = None, description: str = None) -> Optional[Dict]:
        updates, params = [], {"uri": class_uri}
        if title:
            updates.append("c.title = $title")
            params["title"] = title
        if description:
            updates.append("c.description = $description")
            params["description"] = description
        if not updates:
            return self.get_class(class_uri)
        with self.driver.session() as session:
            r = session.run(f"MATCH (c:Class {{uri: $uri}}) SET {', '.join(updates)} RETURN c", params).single()
            return self._collect_node(r["c"]) if r else None

    def create_class(self, title: str, description: str = "", parent_uri: str = None) -> Dict:
        uri = self.generate_random_string()
        with self.driver.session() as session:
            r = session.run(f"CREATE (c:Class:`{uri}` {{uri: $uri, title: $title, description: $description}}) RETURN c", uri=uri, title=title, description=description).single()
            obj = self._collect_node(r["c"])
            if parent_uri:
                session.run("MATCH (c:Class {uri: $uri}), (p:Class {uri: $parent}) CREATE (c)-[:rdfs_subClassOf]->(p)", uri=uri, parent=parent_uri)
                obj["parent_uri"] = parent_uri
        return obj

    def delete_class(self, class_uri: str):
        with self.driver.session() as session:
            uris = [r["uri"] for r in session.run("MATCH (c:Class {uri: $uri}) OPTIONAL MATCH (c)-[:rdfs_subClassOf*0..]->(child:Class) RETURN DISTINCT child.uri as uri", uri=class_uri) if r["uri"]]
            for u in uris:
                session.run("MATCH (o:Object)-[:rdf_type]->(c:Class {uri: $uri}) DETACH DELETE o", uri=u)
                session.run("MATCH (c:Class {uri: $uri}) DETACH DELETE c", uri=u)

    def add_class_attribute(self, class_uri: str, attr_name: str, attr_type: str = "string") -> Dict:
        uri = self.generate_random_string()
        with self.driver.session() as session:
            r = session.run("MATCH (c:Class {uri: $class_uri}) CREATE (dp:DatatypeProperty {uri: $uri, title: $name, type: $type}) CREATE (c)-[:domain]->(dp) RETURN dp", class_uri=class_uri, uri=uri, name=attr_name, type=attr_type).single()
            return self._collect_node(r["dp"])

    def delete_class_attribute(self, class_uri: str, attr_uri: str):
        with self.driver.session() as session:
            session.run("MATCH (c:Class {uri: $class_uri})-[d:domain]->(dp:DatatypeProperty {uri: $attr_uri}) DELETE d", class_uri=class_uri, attr_uri=attr_uri)
            session.run("MATCH (dp:DatatypeProperty {uri: $uri}) WHERE NOT ()-[:domain]->(dp) DETACH DELETE dp", uri=attr_uri)

    def add_class_object_attribute(self, class_uri: str, attr_name: str, range_class_uri: str) -> Dict:
        uri = self.generate_random_string()
        with self.driver.session() as session:
            r = session.run("MATCH (c:Class {uri: $class_uri}), (rc:Class {uri: $range_uri}) CREATE (op:ObjectProperty {uri: $uri, title: $name}) CREATE (c)-[:domain]->(op) CREATE (op)-[:range]->(rc) RETURN op", class_uri=class_uri, range_uri=range_class_uri, uri=uri, name=attr_name).single()
            return self._collect_node(r["op"])

    def delete_class_object_attribute(self, object_property_uri: str):
        with self.driver.session() as session:
            r = session.run("MATCH (op:ObjectProperty {uri: $uri}) RETURN op.title as title", uri=object_property_uri).single()
            if r and r["title"]:
                session.run(f"MATCH ()-[r:`{r['title']}`]->() DELETE r")
            session.run("MATCH (op:ObjectProperty {uri: $uri}) DETACH DELETE op", uri=object_property_uri)

    def add_class_parent(self, parent_uri: str, target_uri: str):
        with self.driver.session() as session:
            session.run("MATCH (c:Class {uri: $target}), (p:Class {uri: $parent}) CREATE (c)-[:rdfs_subClassOf]->(p)", target=target_uri, parent=parent_uri)

    def get_object(self, object_uri: str) -> Optional[Dict]:
        query = "MATCH (o:Object {uri: $uri}) OPTIONAL MATCH (o)-[:rdf_type]->(c:Class) OPTIONAL MATCH (o)-[r]->(t) WHERE NOT type(r) = 'rdf_type' RETURN o, c.uri as class_uri, c.title as class_title, collect({rel: type(r), target: t.uri}) as rels"
        with self.driver.session() as session:
            r = session.run(query, uri=object_uri).single()
            if r:
                obj = self._collect_node(r["o"])
                obj.update({"class_uri": r["class_uri"], "class_title": r["class_title"], "relations": [x for x in r["rels"] if x["rel"] and x["target"]]})
                return obj
        return None

    def delete_object(self, object_uri: str):
        with self.driver.session() as session:
            session.run("MATCH (o:Object {uri: $uri}) DETACH DELETE o", uri=object_uri)

    def collect_signature(self, class_uri: str) -> Dict[str, Any]:
        result = {"class_uri": class_uri, "datatype_properties": [], "object_properties": []}
        with self.driver.session() as session:
            for r in session.run("MATCH (c:Class {uri: $uri}) OPTIONAL MATCH (c)-[:rdfs_subClassOf*0..]->(p:Class)-[:domain]->(dp:DatatypeProperty) RETURN DISTINCT dp.uri as uri, dp.title as title, dp.type as type", uri=class_uri):
                if r["uri"]:
                    result["datatype_properties"].append({"uri": r["uri"], "title": r["title"], "type": r["type"] or "string"})
            for r in session.run("MATCH (c:Class {uri: $uri}) OPTIONAL MATCH (c)-[:rdfs_subClassOf*0..]->(p:Class)-[:domain]->(op:ObjectProperty) OPTIONAL MATCH (op)-[:range]->(rc:Class) RETURN DISTINCT op.uri as uri, op.title as title, rc.uri as range_uri, rc.title as range_title", uri=class_uri):
                if r["uri"]:
                    result["object_properties"].append({"uri": r["uri"], "title": r["title"], "range_class_uri": r["range_uri"], "range_class_title": r["range_title"]})
        return result

    def create_object(self, class_uri: str, title: str, description: str = "", attributes: Dict[str, Any] = None, relations: Dict[str, str] = None) -> Dict:
        uri = self.generate_random_string()
        attributes, relations = attributes or {}, relations or {}
        with self.driver.session() as session:
            r = session.run(f"MATCH (c:Class {{uri: $class_uri}}) CREATE (o:Object:`{class_uri}`:`{uri}` {{uri: $uri, title: $title, description: $desc}}) CREATE (o)-[:rdf_type]->(c) SET o += $attrs RETURN o", class_uri=class_uri, uri=uri, title=title, desc=description, attrs=attributes).single()
            obj = self._collect_node(r["o"])
            for rel, target in relations.items():
                session.run(f"MATCH (o:Object {{uri: $uri}}), (t:Object {{uri: $target}}) CREATE (o)-[:`{rel}`]->(t)", uri=uri, target=target)
        return obj

    def update_object(self, object_uri: str, title: str = None, description: str = None, attributes: Dict[str, Any] = None, relations_to_add: Dict[str, str] = None, relations_to_delete: List[str] = None):
        relations_to_add, relations_to_delete = relations_to_add or {}, relations_to_delete or []
        with self.driver.session() as session:
            updates, params = [], {"uri": object_uri}
            if title:
                updates.append("o.title = $title")
                params["title"] = title
            if description:
                updates.append("o.description = $description")
                params["description"] = description
            if updates:
                session.run(f"MATCH (o:Object {{uri: $uri}}) SET {', '.join(updates)}", params)
            if attributes:
                session.run("MATCH (o:Object {uri: $uri}) SET o += $attrs", uri=object_uri, attrs=attributes)
            for rel in relations_to_delete:
                session.run(f"MATCH (o:Object {{uri: $uri}})-[r:`{rel}`]->() DELETE r", uri=object_uri)
            for rel, target in relations_to_add.items():
                session.run(f"MATCH (o:Object {{uri: $uri}}), (t:Object {{uri: $target}}) CREATE (o)-[:`{rel}`]->(t)", uri=object_uri, target=target)
        return self.get_object(object_uri)


if __name__ == "__main__":
    repo = Neo4jOntologyRepository("bolt://127.0.0.1:7687", "neo4j", "1112311123", "http://university-lab.com/")
    
    print("Очистка базы")
    repo.clear_database()
    
    print("Создание классов")
    person = repo.create_class("Person", "Базовый класс")
    student = repo.create_class("Student", "Студент", parent_uri=person['uri'])
    course = repo.create_class("Course", "Курс")
    
    print("Добавление атрибутов")
    repo.add_class_attribute(person['uri'], "age", "number")
    repo.add_class_object_attribute(student['uri'], "studies", course['uri'])
    
    print("Создание объектов")
    s1 = repo.create_object(student['uri'], "Иванов", attributes={"age": 20})
    c1 = repo.create_object(course['uri'], "Базы данных")
    repo.update_object(s1['uri'], relations_to_add={"studies": c1['uri']})
    
    print("Проверка signature")
    sig = repo.collect_signature(student['uri'])
    print(f"DatatypeProperties: {[dp['title'] for dp in sig['datatype_properties']]}")
    print(f"ObjectProperties: {[op['title'] for op in sig['object_properties']]}")
    
    print("Получение онтологии")
    ont = repo.get_ontology()
    print(f"Классов: {len(ont['classes'])}, Объектов: {len(ont['objects'])}")
    
    repo.close()
