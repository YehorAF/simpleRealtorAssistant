import argparse
from datetime import datetime
import json
import spacy
import re
import pymongo
from pymongo.database import Database
import logging


class Chat:
    def __init__(self, path_to_patterns: str, core = "uk_core_news_sm") -> None:
        self._nlp = spacy.load(core)
        
        with open(path_to_patterns) as fp:
            self._patterns = json.load(fp)

        self._search_action = (
            f"{self._patterns['rverbs']}"
            "[\w\s]{0,20}"
            f"{self._patterns['raction_words']}"
        )


    def normalize(self, query: str) -> list[str]:
        doc = self._nlp(re.sub("(\n|\t)", "", query.lower()))
        lemmas = [tok.lemma_ for tok in doc]
        filtered_sentence = []
        
        for word in lemmas:
            lex = self._nlp.vocab[word]
            if not lex.is_punct and not lex.is_stop:
                filtered_sentence.append(word)

        return filtered_sentence
    

    def split_on_parts(
        self, splited_sentence: list[str]
    ) -> tuple[list[str], tuple[str, str]]:
        joined_sentence = " ".join(splited_sentence)
        action_words = re.search(self._search_action, joined_sentence)

        if not action_words:
            logging.error(f"Cannot find pattern for {splited_sentence}")
            raise ValueError("Не вдалось знайти патерн за вашим запитом")

        action_words = action_words.group(0)
        fields = joined_sentence.replace(action_words, "").split(" ")
        action_words = action_words.split(" ")

        return (
            list(filter(lambda v: v, fields)), 
            (action_words[0], action_words[-1])
        )


    def split_fields(self, query: list[str]) -> dict[str, str]:
        fields = {}
        row = []

        try:
            for word in query:
                if word in self._patterns["field_words"]:
                    if row:
                        ins = fields.get(row[0]) or []
                        fields |= {row[0]: row[1] + ins}
                        row.clear()
                    row = [self._patterns[word], []]
                else:
                    row[1].append(word)
        except:
            logging.error(f"Cannot process query: {query}")
            raise ValueError(
                "На жаль, не можемо обробити ваш запити. "
                "Будь ласка, перепишіть його"
            )

        if row:
            fields |= {row[0]: row[1]}

        return fields


    def fix_search_fields(
        self, fields: dict[str, str | list[str]]
    ) -> dict[str, str | list[str]]:
        nfields = fields.copy()

        for k in fields.keys():
            if k == "description":
                ins = nfields.get("norm_description") or []
                nfields |= {"norm_description": {"$elemMatch": {"$in": nfields[k] + ins}}}
                try: 
                    del nfields[k]
                except: 
                    pass
            elif k == "fullname":
                nfields |= {k: {"$regex": " ".join(nfields[k]), "$options": "i"}}
            elif k == "address":
                ins = nfields.get("norm_address") or []
                nfields |= {"norm_address": {"$elemMatch": {"$in": nfields[k] + ins}}}
                try: 
                    del nfields[k]
                except: 
                    pass
            elif k in ["timestamp", "level"]:
                nfields |= {k: {"$in": nfields[k]}}
            elif k == "price":
                joined_price = " ".join(nfields[k])
                values = re.findall("\d+", joined_price)
                logging.info(values)
                values = [int(value) for value in values]
                nfields |= {
                    k: {"$gte": min(values), "$lte": max(values)}
                }

        logging.info(f"Fields: {nfields}")
        return nfields
    

    def fix_insertion_fields(
        self, fields: dict[str, str | list[str]], collection: str
    ):
        if (collection == "appartments" and 
            len({"address", "price"}.intersection(fields.keys())) != 2):
            raise ValueError(
                "Для додавання нової нерухомості необхідно вказати "
                "ціну й адресу"
            )
        elif (collection == "requests" and 
              len({"fullname"}.intersection(fields.keys())) != 1):
            raise ValueError(
                "Для запиту рієлтору необхідно обов'язково внести свій ПІБ"
            )
        
        nfields = fields.copy()

        description = nfields.get("description") or []
        if description:
            description = self.normalize(" ".join(description))
            nfields |= {"norm_description": description}

        address = nfields.get("address") or []
        if address:
            address = self.normalize(" ".join(address))
            nfields |= {"norm_description": address}

        tags = description + address
        if tags:
            nfields |= {"tags": tags}

        nfields |= {"timestamp": datetime.now().strftime("%d.%m.%y")}

        return nfields


    def make_query(
        self, 
        db: Database, 
        status: str, 
        verb: str, 
        word: str, 
        fields: dict[str, str]
    ):
        logging.info(f"make query with word {word} and verb {verb}")
        if word in self._patterns["realty"]:
            collection = "appartments"
        elif word in self._patterns["worker"]:
            collection = "workers"
        elif word in self._patterns["request"]:
            collection = "requests"
        else:
            logging.error(f"cannot find by word {word}")
            raise ValueError(
                "Не було знайдено патерну, який би відповідав вашому запиту"
            )

        if verb in self._patterns["get_verbs"]:
            func = "select"
        elif verb in self._patterns["insert_verbs"]:
            func = "insert"
        else:
            logging.error(f"cannot find by verb {verb}")
            raise ValueError(
                "Не було знайдено патерну, який би відповідав вашому запиту"
            )

        if (status == "customer" and 
            func == "select" and 
            collection in ["appartments", "workers"]):
            fields = self.fix_search_fields(fields)
            return db[collection].find(fields), collection, func
        elif (status == "customer" and 
              func == "insert" and 
              collection == "requests"):
            fields = self.fix_insertion_fields(fields, collection)
            return db[collection].insert_one(fields), collection, func
        elif (status == "realtor" and 
              func == "select" and 
              collection in ["requests", "appartments"]):
            fields = self.fix_search_fields(fields)
            return db[collection].find(fields), collection, func
        elif (status == "realtor" and 
              func == "insert" and 
              collection == "appartments"):
            fields = self.fix_insertion_fields(fields, collection)
            return db[collection].insert_one(fields), collection, func
        else:
            logging.error(
                f"cannot find by collection {collection} and func {func}"
            )
            raise ValueError(
                "Не було знайдено патерну, який би відповідав вашому запиту"
            )


    def is_quit(self, query: list[str]):
        joined_query = " ".join(query)

        if re.search(self._patterns["rquit_verbs"], joined_query):
            return True
        
        return False


    def format_response(self, result, collection, func):
        if func == "select":
            text = "Було знайдено наступні дані:\n\n"
            for i, data in enumerate(result):
                if collection == "appartments":
                    address = data.get("address")
                    price = data.get("price")
                    description = data.get("description")
                    fullname = data.get("fullname")
                    timestamp = data.get("timestamp")

                    text += (
                        f"{i + 1}.\n"
                        f"Адреса: {address or '-'}\n"
                        f"Ціна: {price or '-'}\n"
                        f"Опис: {description or '-'}\n"
                        f"Рієлтор: {fullname or '-'}\n"
                        f"Час публікації: {timestamp or '-'}\n\n"
                    )
                elif collection == "workers":
                    fullname = data.get("fullname")
                    description = data.get("description")
                    level = data.get("level")

                    text += (
                        f"{i + 1}.\n"
                        f"Рієлтор: {fullname or '-'}\n"
                        f"Опис: {description or '-'}\n"
                        f"Рейтинг: {level or '-'}\n\n"
                    )
                elif collection == "requests":
                    address = data.get("address")
                    price = data.get("price")
                    description = data.get("description")
                    fullname = data.get("fullname")
                    timestamp = data.get("timestamp")

                    text += (
                        f"{i + 1}.\n"
                        f"Адреса: {address or '-'}\n"
                        f"Ціна: {price or '-'}\n"
                        f"Опис: {description or '-'}\n"
                        f"Замовник: {fullname or '-'}\n"
                        f"Час запиту: {timestamp or '-'}\n\n"
                    )
                else:
                    raise ValueError("Не було знайдено подібної колекції")
        elif func == "insert":
            if collection == "requests":
                text = (
                    "Ваш запит було успішно опрацьовано! "
                    "З вами зв'яжуться протягом робочого дня.\n"
                )
            else:
                text = (
                    f"Було успішно внесено запит у колекцію {collection} "
                    f"з ідентифікатором {result.inserted_id}\n"
                )
        else:
            raise ValueError("Не було знайдено подібної дії")
        
        return text


def main():
    logging.basicConfig(
        filename="stat.log",
        format="%(asctime)s - %(message)s", 
        datefmt="%d-%b-%y %H:%M:%S",
        level=logging.INFO
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--role", default="customer")
    
    args = parser.parse_args()
    role = args.role
    if role not in ["customer", "realtor"]:
        raise ValueError(
            """
            Не було знайдено подібної ролі. Оберіть роль з перелічених:
                - customer
                - realtor
            """
        )

    chat = Chat("patterns.json")
    client = pymongo.MongoClient("mongodb://localhost:27017/realty_lab")
    db = client["realty_lab"]

    logging.info("Start")
    print("Вітаємо в тестовому боті для операцій з нерухомістю")

    while True:
        text = input("Введіть запит: ")
        normalized = chat.normalize(text)

        if chat.is_quit(normalized):
            print("До побачення!")
            break

        try:
            logging.info(f"Start procces query: {normalized}")
            fields, (verb, search_word) = chat.split_on_parts(normalized)
            fields = chat.split_fields(fields)
            logging.info(f"Fields from query: {fields}")
            results, collection, func = chat.make_query(
                db, role, verb, search_word, fields)
            logging.info(f"Result: {results}")
            text = chat.format_response(results, collection, func)
            print(text)
        except Exception as ex_:
            print(f"Помилка: {ex_}")

    client.close()
    logging.info("Finish")


if __name__ == "__main__":
    main()