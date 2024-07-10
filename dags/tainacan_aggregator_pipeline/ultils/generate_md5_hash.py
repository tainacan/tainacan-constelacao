import hashlib
import json


def generate_md5_hash(obj, properties=None):
    """
    Gera um hash MD5 de um objeto.

    :param obj: O objeto do qual gerar o hash.
    :param properties: Lista opcional de propriedades a serem utilizadas para gerar o hash.
    :return: String do hash MD5.
    """

    def get_relevant_data(o, props):
        if props is None:
            return o
        if isinstance(o, dict):
            return {k: get_relevant_data(v, props[k]) for k, v in o.items() if k in props}
        elif isinstance(o, list):
            return [get_relevant_data(item, props) for item in o]
        else:
            return o

    def convert_values_to_string(d):
        # Verifica se o objeto é um dicionário
        if isinstance(d, dict):
            return {k: str(v) for k, v in d.items()}
        # Verifica se o objeto é uma lista
        elif isinstance(d, list):
            return [convert_values_to_string(item) for item in d]
        else:
            return str(d)

    relevant_data = get_relevant_data(obj, properties)
    relevant_data = convert_values_to_string(relevant_data)
    # print("DEBUG-----")
    # print(relevant_data)

    # Serializa o objeto para uma string JSON determinística
    serialized_obj = json.dumps(relevant_data, sort_keys=True)

    # Gera o hash MD5
    hash_md5 = hashlib.md5(serialized_obj.encode('utf-8')).hexdigest()

    return hash_md5


# Exemplo de uso
# obj1 = {
#     'name': 'Alice',
#     'age': 30,
#     'address': {'city': 'Wonderland', 'zipcode': '12345'},
#     'emails': ['alice@example.com', 'alice@wonderland.com']
# }

# obj2 = {
#     'name': 'Alice',
#     'age': 30,
#     'address': {'city': 'Wonderland', 'zipcode': '123456'},
#     'emails': ['alice@example.com', 'alice@wonderland.com']
# }

# # Gera hash utilizando todas as propriedades
# hash1 = generate_md5_hash(obj1)
# hash2 = generate_md5_hash(obj2)

# print("Hash1:", hash1)
# print("Hash2:", hash2)
# print("Os hashes são iguais?", hash1 == hash2)

# # Gera hash utilizando apenas algumas propriedades
# properties_to_include = {
#     'name': None,
#     'address': {
#         'city': None
#     }
# }

# hash3 = generate_md5_hash(obj1, properties_to_include)
# hash4 = generate_md5_hash(obj2, properties_to_include)

# print("Hash3:", hash3)
# print("Hash4:", hash4)
# print("Os hashes são iguais?", hash3 == hash4)

# obj1 = {
#     'id': 10669,
#     'museu': 'Museu das Missões',
#     'numero-registro': '181'
# }
# obj2 = {'id': 10669, 'numero-registro': '181', 'museu': 'Museu das Missões'}
# obj3 = {'id': '10669', 'museu': 'Museu das Missões', 'numero-registro': '181'}
# # Gera hash utilizando todas as propriedades
# hash1 = generate_md5_hash(obj1)
# hash2 = generate_md5_hash(
#     obj2, {"id": None, "museu": None, "numero-registro": None})
# hash3 = generate_md5_hash(
#     obj3, {"id": None, "museu": None, "numero-registro": None})

# print("Hash1:", hash1)
# print("Hash2:", hash2)
# print("Hash3:", hash3)
# print("Os hashes são iguais?", hash1 == hash2)
