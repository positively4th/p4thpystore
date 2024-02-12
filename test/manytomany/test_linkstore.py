import unittest
import ramda as R

from contrib.pyas.src.pyas_v3 import As

from src.manytomany.linkstore import LinkStore
from src.store import Store
from test.storetesttools import StoreTestTools
from test.testdb import TestDB
from test.dictstore import DictStore


class TestLinkStore(unittest.IsolatedAsyncioTestCase):

    @classmethod
    def sortById(cls, items: list | tuple | None) -> list | tuple | None:
        if items is None:
            return None

        return R.sort_by(lambda i: i['id'], items)

    async def test_CRUD(self):

        def createSelect(db, idKey):

            async def helper(ids, *args, **kwargs):
                return db.query(
                    predicate=lambda item: idKey in item and item[idKey] in ids
                )

            return helper

        db = As(TestDB)({})

        listenerBookAuthorStoree, flusherBookAuthor = StoreTestTools.createEventListenerAndFlusher()
        authorBookStoree = As(LinkStore, DictStore, Store,
                              *Store.prototypes)({'db': db, 'name': 'BookAuthor'})
        authorBookStoree.registerEventCallback(listenerBookAuthorStoree)

        listenerAuthorStoree, flusherAuthor = StoreTestTools.createEventListenerAndFlusher()
        authorStoree = authorBookStoree.createFacetStoree(
            idGetter=lambda item, *
            args, **kwargs: item['authorId'] if 'authorId' in item else None,
            select=createSelect(db, 'authorId')
        )
        authorStoree.registerEventCallback(listenerAuthorStoree)

        listenerBookStoree, flusherBook = StoreTestTools.createEventListenerAndFlusher()
        bookStoree = authorBookStoree.createFacetStoree(
            idGetter=lambda item, *
            args, **kwargs: item['bookId'] if 'bookId' in item else None,
            select=createSelect(db, 'bookId')
        )
        bookStoree.registerEventCallback(listenerBookStoree)

        expLoadedMap = {}
        authorIdIdsMap = {}
        bookIdIdsMap = {}

        # Test missing
        #
        expItem = None
        actItem = await authorBookStoree.get('P')
        expLoadedMap.update({'P': None})

        self.assertEqual(expItem, actItem)
        actItem = await bookStoree.get('P')
        self.assertEqual([] if expItem is None else [expItem], actItem)
        self.assertEqual(expLoadedMap, authorBookStoree['loadedMap'])

        # Test add to factes store
        #
        exp = {
            'id': '1_1',
            'bookId': 'b1',
            'authorId': 'a1',
        }
        expLoadedMap.update({exp['id']: exp})
        authorIdIdsMap.update({exp['authorId']: set([exp['id']])})
        bookIdIdsMap.update({exp['bookId']: set([exp['id']])})

        act = await authorBookStoree.save(exp)
        self.assertEqual(exp, act)

        self.assertEqual(expLoadedMap, authorBookStoree['loadedMap'])
        self.assertEqual(bookIdIdsMap, bookStoree['facetIdIdsMap'])
        self.assertEqual(authorIdIdsMap, authorStoree['facetIdIdsMap'])

        act = await authorBookStoree.get(exp['id'])
        self.assertEqual(exp, act)

        act = await bookStoree.get(exp['bookId'])
        self.assertEqual([exp], act)

        act = await authorStoree.get(exp['authorId'])
        self.assertEqual([exp], act)
        act = await authorStoree.get(exp['authorId'])
        self.assertEqual([exp], act)

        # Test add to a facets store
        #
        exp = {
            'id': '2_2',
            'bookId': 'b2',
            'authorId': 'a2',
        }
        expLoadedMap.update({exp['id']: exp})
        authorIdIdsMap.update({exp['authorId']: set([exp['id']])})
        bookIdIdsMap.update({exp['bookId']: set([exp['id']])})

        act = await bookStoree.save(exp)
        self.assertEqual(exp, act)

        self.assertEqual(expLoadedMap, authorBookStoree['loadedMap'])
        self.assertEqual(authorIdIdsMap,
                         authorStoree['facetIdIdsMap'])
        self.assertEqual(bookIdIdsMap,
                         bookStoree['facetIdIdsMap'])

        act = await bookStoree.get(exp['bookId'])
        self.assertEqual([exp], act)
        act = await authorBookStoree.get(exp['id'])
        self.assertEqual(exp, act)
        act = await authorStoree.get(exp['authorId'])
        self.assertEqual([exp], act)

        # Test update to a facets store
        #
        expOld = {**exp}
        exp = {
            'id': '2_2',
            'bookId': 'bb2',
            'authorId': 'aa2',
        }
        expLoadedMap.update({exp['id']: exp})
        authorIdIdsMap.pop(expOld['authorId'], None)
        authorIdIdsMap.update({exp['authorId']: set([exp['id']])})
        bookIdIdsMap.pop(expOld['bookId'], None)
        bookIdIdsMap.update({exp['bookId']: set([exp['id']])})

        act = await authorBookStoree.save(exp)
        self.assertEqual(exp, act)

        self.assertEqual(expLoadedMap, authorBookStoree['loadedMap'])
        self.assertEqual(authorIdIdsMap,
                         authorStoree['facetIdIdsMap'])
        self.assertEqual(bookIdIdsMap,
                         bookStoree['facetIdIdsMap'])

        act = await bookStoree.get(exp['bookId'])
        self.assertEqual([exp], act)
        act = await authorBookStoree.get(exp['id'])
        self.assertEqual(exp, act)
        act = await authorStoree.get(exp['authorId'])
        self.assertEqual([exp], act)

        # Test update to a book store
        #
        expOld = {**exp}
        exp = {
            'id': '2_2',
            'bookId': 'bbb2',
            'authorId': 'aaa2',
        }
        expLoadedMap.update({exp['id']: exp})
        authorIdIdsMap.pop(expOld['authorId'], None)
        authorIdIdsMap.update({exp['authorId']: set([exp['id']])})
        bookIdIdsMap.pop(expOld['bookId'], None)
        bookIdIdsMap.update({exp['bookId']: set([exp['id']])})

        act = await bookStoree.save(exp)
        self.assertEqual(exp, act)

        self.assertEqual(expLoadedMap, authorBookStoree['loadedMap'])
        self.assertEqual(authorIdIdsMap,
                         authorStoree['facetIdIdsMap'])
        self.assertEqual(bookIdIdsMap,
                         bookStoree['facetIdIdsMap'])

        act = await bookStoree.get(exp['bookId'])
        self.assertEqual([exp], act)
        act = await authorBookStoree.get(exp['id'])
        self.assertEqual(exp, act)
        act = await authorStoree.get(exp['authorId'])
        self.assertEqual([exp], act)

        # Test delete from facets store
        #
        expOld = {**exp}
        exp = None
        expLoadedMap.pop(expOld['id'], None)
        authorIdIdsMap.pop(expOld['authorId'], None)
        bookIdIdsMap.pop(expOld['bookId'], None)

        act = await authorBookStoree.delete(expOld['id'])
        self.assertEqual(expOld, act)

        self.assertEqual(expLoadedMap, authorBookStoree['loadedMap'])
        self.assertEqual(authorIdIdsMap,
                         authorStoree['facetIdIdsMap'])
        self.assertEqual(bookIdIdsMap,
                         bookStoree['facetIdIdsMap'])

        expLoadedMap.update({expOld['id']: None})
        act = await bookStoree.get(expOld['bookId'])
        self.assertEqual([] if expItem is None else [expItem], act)
        act = await authorBookStoree.get(expOld['id'])
        self.assertEqual(exp, act)
        act = await authorStoree.get(expOld['authorId'])
        self.assertEqual([] if expItem is None else [expItem], act)
        self.assertEqual(expLoadedMap, authorBookStoree['loadedMap'])
        self.assertEqual(authorIdIdsMap,
                         authorStoree['facetIdIdsMap'])
        self.assertEqual(bookIdIdsMap,
                         bookStoree['facetIdIdsMap'])

        # Test delete from book store
        #
        exp = await authorBookStoree.get('1_1')
        self.assertIsNotNone(exp)
        self.assertEqual('1_1', exp['id'])
        expOld = {**exp}
        exp = None
        expLoadedMap.pop(expOld['id'], None)
        authorIdIdsMap.pop(expOld['authorId'], None)
        bookIdIdsMap.pop(expOld['bookId'], None)

        act = await bookStoree.delete(expOld['bookId'])
        self.assertEqual([expOld], act)

        self.assertEqual(expLoadedMap, authorBookStoree['loadedMap'])
        self.assertEqual(authorIdIdsMap,
                         authorStoree['facetIdIdsMap'])
        self.assertEqual(bookIdIdsMap,
                         bookStoree['facetIdIdsMap'])

        act = await bookStoree.get(expOld['bookId'])
        self.assertEqual([] if expItem is None else [expItem], act)
        act = await authorBookStoree.get(expOld['id'])
        self.assertEqual(exp, act)
        act = await authorStoree.get(expOld['authorId'])
        self.assertEqual([] if expItem is None else [expItem], act)
        expLoadedMap.update({expOld['id']: None})
        act = await bookStoree.get(expOld['bookId'])
        self.assertEqual([] if expItem is None else [expItem], act)
        act = await authorBookStoree.get(expOld['id'])
        self.assertEqual(exp, act)
        act = await authorStoree.get(expOld['authorId'])
        self.assertEqual([] if expItem is None else [expItem], act)
        self.assertEqual(expLoadedMap, authorBookStoree['loadedMap'])
        self.assertEqual(authorIdIdsMap,
                         authorStoree['facetIdIdsMap'])
        self.assertEqual(bookIdIdsMap,
                         bookStoree['facetIdIdsMap'])
        return


if __name__ == '__main__':

    unittest.main()
