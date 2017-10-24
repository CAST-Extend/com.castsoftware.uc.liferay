'''
Created on 1 déc. 2015

What's new in 1.4.5 :

- corrects an issue with create_link

What's new in 1.4.4 :

- corrects get_path for UA files with preprocessor

What's new in 1.4.3 :

- correction of a display issue 
- correction of SCRAIP-9522 : call to create_link inside a loop on search object crashes

What's new in 1.4.2 :

- correction of bugs on save_violation
- correction of position when new lines in ReferenceFinder patterns

What's new in 1.4.1 :

- better doc for get_databases, get_owners, ...
- better doc for save_violation
- correction for SCRAIP-8734 : File.get_path() returns None for some dotnet files
  
- access db2zos objects 
  - due to a bug, those objects where not accessed though API
  
What's new in 1.4.0 :

- an application level extension can now add properties to existing objects

The first step is to declare the property you will set on which object types :

    # here say that we take in charge to put the value of the property 'CAST_SQL_Object.version' for 
    # all objects of type 'CAST_MSTSQL_RelationalTable' 
    apllication.declare_property_ownership('CAST_SQL_Object.version',['CAST_MSTSQL_RelationalTable'])
    
   
Then latter you can save that property on an object of those type :    
    
    my_object.save_property('CAST_SQL_Object.version', "my version")


Objects of those types who have not receive a value for that property will do not have the property in the KB.


class Application:

    def declare_property_ownership(self, prop, type_names):
        """
        State that the current plugin handles the production of a property for some types.
        
        The current plugin will calculate all the values for that property for all the objects of those types.
        
        Necessary for saving properties.
        
        :param prop: the property to save. Either a string for the fullname of the property or an integer for the property id. 
        :param types: list of strings the names of the types or categories
        
        """

class Object:

    def save_property(self, prop, value):
        """
        Save a property on current object.
        
        :param prop: the property to save. Either a string for the fullname of the property or an integer for the property id. 
        :param value: the value to set, either a integer, a string or a list of those

        The current plugin must  have declared the property has his own.  
        @see cast.application.Application.declare_property_ownership()
        
        """
        
    def save_violation(self, prop, bookmark, additional_bookmarks=[]):
        """
        Add a violation for the given rule.

        prop is the fullname of a Metamodel property

        :param str prop: a property full name that count the number of rule violations
        :param cast.application.Bookmark bookmark: a bookmark to indicate the position of the violation
        :param cast.application.Bookmark additional_bookmarks: additional bookmarks that help
                                                               understanding the violation
        
        The property 'prop' is automatically valorised with the number of violations for the object. So do not use save_property for this property.
        
        The current plugin must have declared the prop has his own.  
        @see cast.application.Application.declare_property_ownership()
        
        """ 

- objects can be used as key in python dictionary
  - 2 objects are equal if they got the same id
- correction for SCRAIP-8734 : File.get_path() returns None for some dotnet files 
- correction for SCRAIP-9522 : call to create_link inside a loop on search object crashes
  - now the actual saving phase is done at the end of the plugin execution
  

What's new in 1.2.0 :
  
class Object:

    def get_fullname(self):
        """
        Returns object fullname.
        """
  
What's new in 1.1.0 :

class Application:

    def search_objects(self, name=None, category=None, load_properties=False):
        """
        Search objects by name or/and by type
        
        :param str name: the name of the searched object
        :param str category: the category name of the searched object
        :param bool load_properties: if True properties of objects will be available. Slower.
        
        :rtype: iterable of :class:`cast.application.Object`
        """

class Object:

    def get_property(self, property):
        """
        Return an object property.
        
        :param str or int or Property property: the property fullname, or property id or property to get
        """


@warning: 

For API maintainers : 
if you modify KnowledgeBase.__init__ or Application.__init__ then you need to do 
special treatment for 8.0. 

@author: MRO
'''
from sqlalchemy import create_engine, MetaData, Table, Column, Sequence, Integer, \
                       String, select, text, TIMESTAMP, outerjoin, literal_column, union, \
                       delete
from sqlalchemy.sql import func                       
from sqlalchemy.exc import NoSuchTableError
from collections import defaultdict
from datetime import date
from cast.application.internal.metamodel import MetaModel, Category, Type, Property
import sqlparse
import logging
import collections
import re
import fileinput
import binascii


"""
Handle extensibility at application level.

"""

def experimental(f):
    """
    Annotation for experimental functions.
    Use with care...
    """
    return f

class KnowledgeBase:
    """
    A connection to a knowledge base.
    """
    def __init__(self, name, engine=None):
        self.engine = engine or create_engine("postgresql+pg8000://operator:CastAIP@localhost:2280/postgres")
        self.name = name
        self.metadata = MetaData(bind=self.engine, schema=name)
        
        self.connection = self.engine.connect()
        self.raw_connection = self.engine.raw_connection()

        schema_exists = True

        try:
            
            if self.engine.dialect.name == 'mssql':
                # usefull tables
                self.Keys = Table("keys",
                                  self.metadata,
                                  Column('idkey',
                                         Integer,
                                         primary_key=True, autoincrement=False),
                                  Column('objtyp', Integer),
                                  Column('keynam', String(255)),
                                  Column('keytyp', String(6)),
                                  Column('keysubtyp', Integer),
                                  Column('keyclass', Integer),
                                  Column('keyprop', Integer),
                                  Column('idusrdevpro', String(3)),
                                  Column('keydevdat', TIMESTAMP),
                                  implicit_returning=False,
                                  autoload=True,
                                  autoload_with=self.engine)
            else:
                # usefull tables
                self.Keys = Table("keys",
                                  self.metadata,
                                  Column('idkey',
                                         Integer,
                                         Sequence('idkey_generator',
                                                  schema=name),
                                         primary_key=True),
                                  Column('objtyp', Integer),
                                  Column('keynam', String(255)),
                                  Column('keytyp', String(6)),
                                  Column('keysubtyp', Integer),
                                  Column('keyclass', Integer),
                                  Column('keyprop', Integer),
                                  Column('idusrdevpro', String(3)),
                                  Column('keydevdat', TIMESTAMP),
                                  implicit_returning=False,
                                  autoload=True,
                                  autoload_with=self.engine)
        
        except NoSuchTableError:
            schema_exists = False 
            
        if not schema_exists:            
            raise RuntimeError("knowledge base '%s' does not exist" % name) 
            
        self.ObjPro = Table("objpro",
                            self.metadata,
                            Column('idobj', Integer),
                            Column('idpro', Integer),
                            Column('prop', Integer),
                            autoload=True,
                            autoload_with=self.engine)
        
        self.ObjFulNam = Table("objfulnam",
                               self.metadata,
                               Column('idobj', Integer),
                               Column('fullname', String(255)),
                               autoload=True,
                               autoload_with=self.engine)
                               
        
        self.RefPath = Table("refpath",
                             self.metadata,
                             Column('idfilref', Integer, primary_key=True),
                             Column('path', String(600)),
                             autoload=True,
                             autoload_with=self.engine)
        
        self.ObjFilRef = Table("objfilref",
                               self.metadata,
                               Column('idobj', Integer),
                               Column('idfilref', Integer),
                               Column('idfil', Integer),
                               autoload=True,
                               autoload_with=self.engine)
        
        # @todo : sql server
        # corrects here : works for 7.3
        # for 8.0 call _additional_init()
        self.ObjPos = Table("objpos",
                            self.metadata,
                            autoload=True,
                            autoload_with=self.engine)
        
        self.KeyPar = Table("keypar",
                            self.metadata,
                            Column('idkey', Integer),
                            Column('idparent', Integer),
                            autoload=True,
                            autoload_with=self.engine)

        self.metamodel = None
        # interesting pointers
        self.project_category = None
        self.database_subset_category = None

        # metamodel caching
        self._load_metamodel()
        # usefull constants
        self.project_category = self.metamodel.get_category(id=1013)
        self.database_subset_category = self.metamodel.get_category(id=140351)
        self.user_project_category = self.metamodel.get_category(id=669)
        self.shell_category = self.metamodel.get_category(id=1014)
        self.directory_category = self.metamodel.get_category(id=5039)
        
        self.__init_database_related_informations__()

        # only in version >= 7.3
        self.plugin_project_type = None
        try:
            self.plugin_project_type = self.metamodel.get_category(id=141887)
        except:
            pass

        self.identification_name = self.metamodel.get_property(id=3)
        self.identification_fullname = self.metamodel.get_property(id=125)
        self.projectRelationKind = self.metamodel.get_property(id=1055)
        self.dependencyKind = self.metamodel.get_property(id=1058)
        
        if self.engine.dialect.name == 'oracle':
            cursor = self.create_cursor()
            self._execute_function(cursor, 'USER_INFO.init_internal_values', "-1, '%s'" % self.engine.url.username)
            self.raw_connection.commit()

    def __init_database_related_informations__(self):
        #
        # compatible with CAST versions >= 7.0
        #
        try:
            self.rootContainer_category = self.metamodel.get_category(id=141169)  # CAST_SQL_RootContainer
        except:
            try:
                self.rootContainer_category = self.metamodel.get_category(id=278)  # DATABASE
            except:
                pass

        try:
            self.instanceContainer_category = self.metamodel.get_category(id=138012)  # CAST_Oracle_Instance or CAST_SQL_Instance (legacy)
        except:
            pass
        try:
            self.ownerContainer_category = self.metamodel.get_category(id=141170)  # CAST_SQL_OwnerContainer
        except:
            try:
                self.ownerContainer_category = self.metamodel.get_category(id=17)  # SQL_SCHEMA
            except:
                pass
        try:
            self.ownerContainer2_category = self.metamodel.get_category(id=138014)  # CAST_Oracle_Schema or CAST_SQL_Schema (legacy)
        except:
            self.ownerContainer2_category = None

    def get_applications(self):
        """
        Returns the list of application of the knowledge base.
        
        :rtype: list of :class:`cast.application.Application`
        
        """
        cursor = self.create_cursor()
        cursor.execute("select IdUsrPro from UsrPro")
        applications = [self._load_object(x[0]) for x in cursor]
        # skipp KB Information Finalization because not an application
        return [app for app in applications if app.get_name() != 'KB Information Finalization']

    def get_application(self, name):
        """
        Access to an application by name.
        
        :param str name: the name of the application

        :rtype: :class:`cast.application.Application`
        """
        for app in self.get_applications():
            if app.get_name() == name:
                return app
            
        return None

    def update_cast_system_views(self):
        """
        Updates the Cast System Views.
        """
        cursor = self.create_cursor()
        
        # execute init

        """        
        LPSTR szCreateTempTable2 = "create table #CSV_INFO (IdKey int null, Mangling varchar(1000) null , Info varchar(1000) null) %s";
        LPSTR szCreateIdxTempTable2 = "create index IdxTemp2 on #CSV_INFO (IdKey)";
        """        
        
        if self.engine.dialect.name == 'mssql':
            
            cursor.execute('create table #CSV_INFO (IdKey int null, Mangling varchar(1000) null , Info varchar(1000) null)')
            cursor.execute('create index IdxTemp2 on #CSV_INFO (IdKey)')
        
        self._execute_function(cursor, 'csv_init')
        self._execute_function(cursor, 'csv_generate_info')

    def create_cursor(self):
        cursor = self.raw_connection.cursor()
        if self.name:
            
            dialect = self.engine.dialect.name

            if dialect == 'postgresql':
                cursor.execute("SET search_path TO %s" % self.name)
            elif dialect == 'oracle':
                cursor.execute("ALTER SESSION SET CURRENT_SCHEMA = %s" % self.name)
                
        return cursor

    def _build_object_wrapper(self, identifier, idtype, name, additional_values=None):
        """
        Correctly subclass Object according to object type
        """
        typ = self.metamodel.get_category(id=idtype)
        
        if typ == self.user_project_category:
            return cast.application.Application(self, identifier, name, typ)
    
        if typ.inherit_from(self.database_subset_category):
            return cast.application.DatabaseSubset(self, identifier, name, typ)
        
        if typ.inherit_from(self.project_category):
            return cast.application.Project(self, identifier, name, typ, additional_values)

        if typ.inherit_from(self.shell_category) or idtype == 512:
            return cast.application.File(self, identifier, name, typ, additional_values)

        try:
            if typ.inherit_from(self.rootContainer_category):
                return cast.application.Database(self, identifier, name, typ, additional_values)
        except:
            pass

        try:
            if typ.inherit_from(self.instanceContainer_category):
                return cast.application.Database(self, identifier, name, typ, additional_values)
        except:
            pass

        try:
            if typ.inherit_from(self.ownerContainer_category):
                return cast.application.DatabaseOwner(self, identifier, name, typ, additional_values)
            else:
                try:
                    if typ.inherit_from(self.ownerContainer2_category):
                        return cast.application.DatabaseOwner(self, identifier, name, typ, additional_values)
                except:
                    pass
        except:
            if typ.inherit_from(self.ownerContainer2_category):
                return cast.application.DatabaseOwner(self, identifier, name, typ, additional_values)
        
        # UDBDATABASE
        if idtype == 313:
            return cast.application.Database(self, identifier, name, typ, additional_values)
        
        # UDBSCHEMA
        if idtype == 301:
            return cast.application.DatabaseOwner(self, identifier, name, typ, additional_values)
        return cast.application.Object(self, identifier, name, typ, additional_values)

    def _load_object(self, identifier):
        """
        Load one object.
        """
        cursor = self.create_cursor()
        cursor.execute("select KeyNam,ObjTyp from Keys where IdKey = %s" % identifier)
        
        line = cursor.fetchone()
        if line:
            obj_name = line[0]
            obj_type = line[1]
            
            return self._build_object_wrapper(identifier, obj_type, obj_name)

    def _load_metamodel(self):
        """
        Load the metamodel.
        """
        mm = MetaModel()
        
        # types
        cursor = self.create_cursor()
        cursor.execute("select IdTyp,TypNam from Typ")
        
        for mm_type in cursor:
            result = Type()
            result.id = mm_type[0]
            result.name = mm_type[1]
            mm._add_type(result)
        
        # categories
        cursor = self.create_cursor()
        cursor.execute("select IdCat,CatNam from Cat")

        for mm_category in cursor:
            result = Category()
            result.id = mm_category[0]
            result.name = mm_category[1]
            mm._add_category(result)

        # type inheritance
        cursor = self.create_cursor()
        cursor.execute("select IdTyp,IdCatParent from TypCat")

        for inheritance in cursor:

            idtyp = inheritance[0]
            idcat = inheritance[1]

            typ = mm.get_category(id=idtyp)
            if typ:
                cat = mm.get_category(id=idcat)
                typ.all_inherited_categories.add(cat)
                cat.sub_categories.add(typ)
                cat.sub_types.add(typ)

        # category inheritance
        cursor = self.create_cursor()
        cursor.execute("select IdCat,IdCatParent from CatCat")

        for inheritance in cursor:

            idtyp = inheritance[0]
            idcat = inheritance[1]

            typ = mm.get_category(id=idtyp)
            if typ:
                cat = mm.get_category(id=idcat)
                typ.all_inherited_categories.add(cat)
                cat.sub_categories.add(typ)

        # properties
        cursor = self.create_cursor()
        cursor.execute("select idprop,propnam,proptyp,cardmin,cardmax from prop")
        for prop in cursor:

            result = Property()
            result.id = prop[0]

            # the small name in fact
            result.name = prop[1]

            predefined_type_names = {137475: 'integer',
                                     137476: 'string',
                                     137477: 'bookmark',
                                     137478: 'dateTime',
                                     1028: 'reference'}

            proptyp = prop[2]
            if proptyp in predefined_type_names:
                result.type = predefined_type_names[proptyp]
            else:
                # id of a category
                result.type = mm.get_category(id=proptyp)

            result.minimal_cardinality = prop[3]
            result.maximal_cardinality = prop[4]

            mm._add_property(result)

        mm.properties_by_name = {}
        # properties of types and categories
        cursor = self.create_cursor()
        cursor.execute("select idtyp,idprop from typprop union select idcat,idprop from propcat")
        for prop in cursor:
            idtyp = prop[0]
            idprop = prop[1]
            typ = mm.get_category(id=idtyp)
            p = mm.get_property(id=idprop)

            # reindex
            p.name = typ.name + '.' + p.name
            mm.properties_by_name[p.name] = p

            typ.properties.add(p)
        self.metamodel = mm

    # last parameter is to enable TSQL legacy where project is database
    def _get_object_query(self, project_ids, internal=True, external=False, exclude_project=True):
        """
        Build a query that returns the objects of projects

        """
        is_in_project = select([self.ObjPro.c.idobj]).where(self.ObjPro.c.idpro.in_(project_ids))

        if internal and not external:
            is_in_project = is_in_project.where(self.ObjPro.c.prop == 0)
        elif not internal and external:
            is_in_project = is_in_project.where(self.ObjPro.c.prop == 1)
        
        key_fullname = outerjoin(self.Keys, self.ObjFulNam, self.Keys.c.idkey == self.ObjFulNam.c.idobj) 

        query = select([self.Keys.c.idkey, self.Keys.c.objtyp, self.Keys.c.keynam, self.ObjFulNam.c.fullname]).select_from(key_fullname)
        # objects of projects
        query = query.where(self.Keys.c.idkey.in_(is_in_project))
        # but not the project themselves
        if exclude_project:
            query = query.where(~self.Keys.c.idkey.in_(project_ids))
        
        return query

    def _execute_query(self, query, application=None):
        """
        Execute a query.
        """
        cursor = self.create_cursor()
        cursor.execute(str(query.compile(compile_kwargs={"literal_binds": True})))
        return (self._build_object_wrapper(o[0], o[1], o[2], {'application':application, 'fullname':o[3]}) for o in cursor)

    def _get_objects(self, project_ids, internal=True, external=False, application=None):
        """
        Get all the objects of a set of projects
        """

        objects_of_projects = self._get_object_query(project_ids, internal, external)
        return self._execute_query(objects_of_projects, application)

    def _get_objects_by_name(self, project_ids, name, internal=True, external=False, application=None):
        """
        Get all the objects of projects with given name
        """

        objects_of_projects = self._get_object_query(project_ids, internal, external)
        query = objects_of_projects.where(self.Keys.c.keynam == name)
        return self._execute_query(query, application)

    def _search_objects(self, project_ids, name=None, category=None, application=None, parent_object=None, exclude_project=True, load_properties=False):
        """
        Search all the objects of projects by name and or by type. 
        If load_properties is true, will load the object properties too.
        """
        self._load_infsub_types()
        
        query = None
        if not load_properties:
            query = self._get_object_query(project_ids, True, False, exclude_project)
        else:
            
            objdsc = Table("objdsc", 
                           self.metadata, 
                           Column('idobj', Integer, primary_key=True),
                           Column('inftyp', Integer, primary_key=True),
                           Column('infsubtyp', Integer, primary_key=True),
                           Column('blkno', Integer, primary_key=True),
                           Column('ordnum', Integer, primary_key=True),
                           Column('prop', Integer),
                           Column('infval', String(255)),
                           autoload=True, 
                           autoload_with=self.engine,
                           keep_existing=True)
            
            objinf = Table("objinf", 
                           self.metadata, 
                           Column('idobj', Integer),
                           Column('inftyp', Integer),
                           Column('infsubtyp', Integer),
                           Column('blkno', Integer),
                           Column('infval', Integer),
                           autoload=True, 
                           autoload_with=self.engine,
                           keep_existing=True)
    
            """
            @todo : 
            - ordnum for multivalues
            - blkno for multi lines strings...
            """
            
            select_dsc = select([objdsc.c.idobj, objdsc.c.inftyp, objdsc.c.infsubtyp, objdsc.c.infval.label('string_value'), literal_column('null').label('int_value'), objdsc.c.blkno])
            select_inf = select([objinf.c.idobj, objinf.c.inftyp, objinf.c.infsubtyp, literal_column('null'), objinf.c.infval, objinf.c.blkno])
            
            properties = union(select_dsc, select_inf).alias('properties')
            j = outerjoin(self.Keys, self.ObjFulNam, self.Keys.c.idkey == self.ObjFulNam.c.idobj) 
            myjoin = outerjoin(j, properties, self.Keys.c.idkey == properties.c.idobj) 
            
            is_in_project = select([self.ObjPro.c.idobj]).where(self.ObjPro.c.idpro.in_(project_ids))
            is_in_project = is_in_project.where(self.ObjPro.c.prop == 0)
    
            query = select([self.Keys.c.idkey, self.Keys.c.objtyp, self.Keys.c.keynam, self.ObjFulNam.c.fullname, properties]).select_from(myjoin)
            # objects of projects
            query = query.where(self.Keys.c.idkey.in_(is_in_project))
            # but not the project themselves
            query = query.where(~self.Keys.c.idkey.in_(project_ids))
    
            query = query.order_by(self.Keys.c.idkey, properties.c.inftyp, properties.c.infsubtyp)
    
        if name:
            query = query.where(self.Keys.c.keynam == name)
    
        if category:
            types = self.metamodel.get_category(name=category).get_sub_types()
            type_ids = [t.id for t in types]
            query = query.where(self.Keys.c.objtyp.in_(type_ids))
        
        if parent_object:
            is_child = select([self.KeyPar.c.idkey]).where(self.KeyPar.c.idparent == parent_object.id)
            query = query.where(self.Keys.c.idkey.in_(is_child))
        
        if not load_properties:
            
            for o in self._execute_query(query, application):
                yield o
            
        else:
            cursor = self.create_cursor()
            cursor.execute(str(query.compile(compile_kwargs={"literal_binds": True})))
            
            current_object = None
            for line in cursor:
                current_object_id = current_object.id if current_object else None
                object_id = line[0]
    
                # true when we have changed object
                is_new_object_line = object_id != current_object_id
                 
                if is_new_object_line:
                    # create object
                    old_object = current_object
                    current_object = self._build_object_wrapper(line[0], line[1], line[2], {'application':application, 'fullname':line[3]})
                     
                # add property to current object
                inftyp = line[5]
                infsubtyp = line[6]
                property = self._search_property(inftyp, infsubtyp)
                string_value = line[7]
                int_value = line[8]
                value = int_value if not int_value is None else string_value
                current_object._add_property_value(property, value)
                
                if is_new_object_line and old_object:
                    yield old_object
            
            if current_object:
                yield current_object

    def _get_files(self, project_ids, languages=[], application=None):
        """
        Get all the files of a project.

        @param languages: a list of categories that filter the file types
        """
        
        # basic query
        query = select([self.Keys.c.idkey,
                        self.Keys.c.objtyp,
                        self.Keys.c.keynam,
                        self.RefPath.c.path,
                        self.ObjFulNam.c.fullname]).select_from(

                        self.Keys.join(self.ObjFilRef,
                                       self.ObjFilRef.c.idobj == self.Keys.c.idkey, 
                                       isouter=True).join(self.RefPath,
                                                          self.RefPath.c.idfilref == self.ObjFilRef.c.idfilref, 
                                                          isouter=True).join(self.ObjFulNam, 
                                                                             self.ObjFulNam.c.idobj == self.Keys.c.idkey, 
                                                                             isouter=True))

        # filter on projects
        is_in_project = select([self.ObjPro.c.idobj]).where(self.ObjPro.c.idpro.in_(project_ids))
        is_in_project = is_in_project.where(self.ObjPro.c.prop == 0)

        query = query.where(self.Keys.c.idkey.in_(is_in_project))

        # filter on types
        # cache it ???
        types = self.shell_category.get_sub_types()
        types.add(self.metamodel.get_category(id=512)) # C_FILE do not inherit from shell...
        type_ids = [t.id for t in types if not t.inherit_from(self.directory_category) and t.inherit_from_one_of(languages)]

        query = query.where(self.Keys.c.objtyp.in_(type_ids))

        # execute and pass additional param
        cursor = self.create_cursor()
        cursor.execute(str(query.compile(compile_kwargs={"literal_binds": True})))
        return (self._build_object_wrapper(o[0], 
                                           o[1], 
                                           o[2], 
                                           {'path': o[3], 
                                            'application':application, 
                                            'fullname':o[4]}) for o in cursor)

    def _check_sub_object(self, f, line):
        """
        Perform checks to determine if the line from a query represent 
        an new object or not
        """
        if f.id == line[0]:
            # this position is for the file
            if not f.positions:
                f.positions = []
            f.positions.append(Bookmark(f,
                                        line[3],
                                        line[4],
                                        line[5],
                                        line[6]))

            return False
        else:
            
            o = f._get_sub_object_by_id(line[0])
            
            if o:
                if not o.positions:
                    o.positions = []
                o.positions.append(Bookmark(f,
                                            line[3],
                                            line[4],
                                            line[5],
                                            line[6]
                                            ))
    
                return False
                
            return True

    def _load_objects(self, f):
        """
        loads the sub objects of a file.

        First try with objpos

        Will not work for synthetised sub objects, but we do not care for now
        """
        query = select([self.Keys.c.idkey,
                        self.Keys.c.objtyp,
                        self.Keys.c.keynam,
                        self.ObjPos.c.info1,
                        self.ObjPos.c.info2,
                        self.ObjPos.c.info3,
                        self.ObjPos.c.info4]).select_from(self.Keys.join(self.ObjPos,
                                                                         self.ObjPos.c.idobj == self.Keys.c.idkey))
        query = query.where(self.ObjPos.c.idobjref == f.id)

        cursor = self.create_cursor()
        cursor.execute(str(query.compile(compile_kwargs={"literal_binds": True})))
        return (self._build_object_wrapper(o[0], o[1], o[2], {'begin_line': o[3],
                                                              'begin_column': o[4],
                                                              'end_line': o[5],
                                                              'end_column': o[6],
                                                              'file': f,
                                                              'application': f.application})
                for o in cursor if self._check_sub_object(f, o))


    def _execute_raw_query(self, cursor, query):
        
        # multi type : string, file, FileInput
        if type(query) is fileinput.FileInput:
            temp = ''.join(line for line in query)
            query = temp
        elif not type(query) is str:
            query = query.read()


        query = replace_special_variables(query)
        # split into several statements because dbapi generally can only
        # execute one statement at a time
        statements = sqlparse.split(query)

        for statement in statements:
            # may split with empty line...
            if statement:
                logging.debug('executing statement : raw  %s', statement)
                t = text(_remove_last_comma(statement))
                
                statement_string = str(t.compile(bind=self.connection))
                logging.debug('executing statement %s', statement_string)
                
                cursor.execute(statement_string)
                self.raw_connection.commit()
        
    def _execute_function(self, cursor, function_call, parameters=''):
        """
        parameters comma separated parameter without parenthesis
        """
        dialect = self.engine.dialect.name
        
        def add_parenthesis(parameters):
            return '(' + parameters + ')'
        
        if dialect == 'oracle':
            query = 'DECLARE error int;  begin error :=' + function_call + add_parenthesis(parameters) + '; end;'
            cursor.execute(query)
        elif dialect == 'postgresql':
            cursor.execute('select ' + function_call + add_parenthesis(parameters))
        elif dialect == 'mssql':
            cursor.execute('exec ' + function_call + ' ' + parameters)
            self.raw_connection.commit()
        
    def _search_property(self, inftyp, infsubtyp):
        """
        Get a property per inftyp/infsubtyp 
        """
        
        m = getattr(self.metamodel, 'prop_per_inftyp')
        
        try:
            return self.metamodel.get_property(id=m[inftyp][infsubtyp])
        except:
            return None
    
    # # patch for KnowledgeBase type
    def _load_infsub_types(self):
        """
        loads the inftyp/infsubtyp for properties
        """
        if not hasattr(self.metamodel, 'prop_per_inftyp'):
            
            result = {}
            
            cursor = self.create_cursor()
            cursor.execute("select IdProp, AttrNam, IntVal from PropAttr where AttrNam = 'INF_TYPE' or  AttrNam = 'INF_SUB_TYPE' order by IdProp, AttrNam")
    
            inf_typ = None
            inf_subtyp = None
    
            for line in cursor:
                
                if line[1] == 'INF_TYPE':
                    inf_typ = line[2]
                    
                    # second line so we can register mapping
                    if not inf_typ in result:
                        result[inf_typ] = {}
                    
                    result[inf_typ][inf_subtyp] = line[0]
                    
                    try:
                        p = self.metamodel.get_property(id=line[0])
                        setattr(p,'inftyp',inf_typ)
                        setattr(p,'infsubtyp',inf_subtyp)
                    except:
                        pass
                    
                else:
                    inf_subtyp = line[2]
            
            
            setattr(self.metamodel, 'prop_per_inftyp', result)
        

class Bookmark:
    """
    A source code area.
    """

    def __init__(self, file, begin_line, begin_column, end_line, end_column):

        self.begin_line = begin_line
        self.begin_column = begin_column
        self.end_line = end_line
        self.end_column = end_column
        self.file = file

    def contains_position(self, line, column):
        """
        True if bookmark contains a single position.
        """
        return line >= self.begin_line and line <= self.end_line and (line > self.begin_line or column >= self.begin_column) and (line < self.end_line or column <= self.end_column)

    def contains(self, position):
        """
        True if bookmark contains another area.
        """
        return self.contains_position(position.begin_line, position.begin_column) and self.contains_position(position.end_line, position.end_column)

    def __repr__(self):
        return 'Bookmark(%s, %s, %s, %s, %s)' % (str(self.file), self.begin_line, self.begin_column, self.end_line, self.end_column)


class Object:
    """
    A KB object of any kind
    """

    def __init__(self, kb, identifier, name, typ, additional_values=None):
        self.kb = kb
        # cached data
        self.id = identifier
        self.name = name
        # metamodel type see cast.application.internal.metamodel.Type
        self.type = typ
        
        # loadable informations
        self.sub_objects = None
        self.positions = None
        
        self.application = None
        self.projects = None
        self.parent = None
        self.children = None
        self.fullname = None
        
        values = ('begin_line',
                  'begin_column',
                  'end_line',
                  'end_column',
                  'file')

        if additional_values and 'fullname' in additional_values:
            self.fullname = additional_values['fullname']

        if additional_values and self._has_additional_values(additional_values, values):

            self.positions = []
            self.positions.append(Bookmark(additional_values['file'],
                                           additional_values['begin_line'],
                                           additional_values['begin_column'],
                                           additional_values['end_line'],
                                           additional_values['end_column']))
        
        if additional_values and self._has_additional_values(additional_values, ['application']):
            
            self.application = additional_values['application']

    def get_name(self):
        """
        Returns object name.
        """
        return self.name

    def get_fullname(self):
        """
        Returns object fullname.
        """
        return self.fullname

    @experimental
    def get_qualified_name(self):
        """
        Give a qualified name usefull for linking. 

        Try to compensate fullnames differences
        
        @since 1.4.0
        """
        
        
        # C++ case        
        Cpp = self.kb.metamodel.get_category(id=140009)
        if self.type.inherit_from(Cpp):
            return '.'.join(re.findall('\[([^\][]+)\]', self.fullname)[1:])
        
        # @todo : handle other cases
        
        # Dotnet, sqls, java
        return self.fullname
        
        
    def get_prefixed_name(self):
        """
        Returns object type.name.
        """
        return self.type.name + '.' + self.name

    def get_type(self):
        """
        Returns object's type name.
        """
        return self.type.get_name()

    def get_property(self, property):
        """
        Return an object property.
        
        :param str or int or Property property: the property fullname, or property id or property to get
        """
        if not hasattr(self, '_properties'):
            raise RuntimeError("Cannot use Object.get_property() if property has not been loaded")
        
        properties = self._properties
        
        if type(property) is str:
            property = self.kb.metamodel.get_property(name=property)
        elif type(property) is int:
            property = self.kb.metamodel.get_property(id=property)
        
        try:
            return properties[property]
        except:
            return None

    def get_positions(self):

        if self.positions is None:
            """
            @todo: load the positions of the object
            """
            pass

        return self.positions

    def save_property(self, prop, value):
        """
        Save a property on current object.
        
        :param prop: the property to save. Either a string for the fullname of the property or an integer for the property id. 
        :param value: the value to set, either a integer, a string or a list of those

        The current plugin must have declared the property has his own.  
        @see cast.application.Application.declare_property_ownership()
        
        """
        _property = self._convert_into_property(prop)
        
        self.application._get_raw_saver().add_property(self, _property, value)

    def save_violation(self, prop, bookmark, additional_bookmarks=[]):
        """
        Add a violation for the given rule.

        prop is the fullname of a Metamodel property

        :param str prop: a property full name that count the number of rule violations
        :param cast.application.Bookmark bookmark: a bookmark to indicate the position of the violation
        :param cast.application.Bookmark additional_bookmarks: additional bookmarks that help
                                                               understanding the violation
        
        The property 'prop' is automatically valorised with the number of violations for the object. So do not use save_property for this property.
        
        The current plugin must have declared the prop has his own.  
        @see cast.application.Application.declare_property_ownership()
        
        """ 
        _property = self._convert_into_property(prop)
        
        self.application._get_raw_saver().add_violation(self, _property, bookmark, additional_bookmarks)


    def get_application(self):
        """
        Returns object's application
        """
        return self.application

    def get_projects(self):
        """
        Returns object's projects
        """
        if not self.projects:
            
            self.projects = self.application._get_projects(self)
                        
        
        return self.projects

    def load_objects(self):
        """
        loads the sub objects of an object.
        """
        if not self.sub_objects:
            return []

        return self.sub_objects

    def find_most_specific_object(self, line, column):
        """
        Find the most specific sub object containing line, column.
        """
        result = self
        result_position = None
        for sub_object in self.load_objects():
            for position in sub_object.get_positions():
                if position.contains_position(line, column) and (not result_position or result_position.contains(position)):
                    result = sub_object
                    result_position = position
                    
        return result
        
    def _has_additional_values(self, additional_values, expected_values):
        
        for value in expected_values:
            if not value in additional_values:
                return False
            
        return True
    
    def _get_sub_object_by_id(self, _id):
        
        if not self.sub_objects:
            return None
        
        for o in self.sub_objects:
            if o.id == _id:
                return o
        
        return None
    
    def load_children(self, categories=None, is_sorted=None):
          
        if not self.children:
            self.children = []
            if categories:
                for cat in categories:
                    for o in self.kb._search_objects(self.application.projects_ids, name=None, category=cat, application=self.application, parent_object=self):
                        self.children.append(o)
                        o.parent = self
            else:
                for o in self.kb._search_objects(self.application.projects_ids, name=None, category=None, application=self.application, parent_object=self):
                    self.children.append(o)
                    o.parent = self
          
        if is_sorted:
            self.children.sort(key=Object.get_prefixed_name)
            
        return self.children

    def get_children(self, categories=None):
        
        if categories:
            return [ child for child in self.children if child.type.inherit_from_one_of(categories) ]
        else:
            return [ child for child in self.children ]
    
    def __repr__(self):
        
        display_name = self.get_qualified_name()
        if not display_name:
            display_name = self.get_name()
            
        return 'Object(%s, %s)' % (display_name, self.get_type())

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id    

    def _add_property_value(self, property, value):
        
        if not hasattr(self, '_properties'):
            setattr(self, '_properties', {})
        
        properties = getattr(self, '_properties')
        if not property in properties:
            
            if property.get_maximal_cardinality() == 1:
                properties[property] = value
            else:
                properties[property] = (value)
        else:
            if property.get_maximal_cardinality() == 1:
                properties[property] = value
            else:
                properties[property].append(value)
    
    def _convert_into_property(self, prop):
        """
        Convert a string, id, etc into a cast.application.internal.metamodel.Property
        """
        try:
            if type(prop) is str:
                return self.kb.metamodel.get_property(name=prop)
            elif type(prop) is int:  
                return self.kb.metamodel.get_property(id=prop)
            elif isinstance(prop, Property):
                return prop
        except:
            raise RuntimeError("Invalid property " + str(prop))

    def _convert_into_types(self, type_names):
        """
        Convert a string, id, list of those into a list of cast.application.internal.metamodel.Type
        """
        def convert_single(_cat):
            
            try:
                cat = None
                if type(_cat) is str:
                    cat = self.kb.metamodel.get_category(name=_cat)
                elif type(_cat) is int:  
                    cat = self.kb.metamodel.get_category(id=_cat)
                if cat:
                    return cat.get_sub_types()
                else:
                    RuntimeError("Invalid type or category " + str(_cat))
            except:
                RuntimeError("Invalid type or category " + str(_cat))
        
        if type(type_names) is list:
            
            result = []
            for _typ in type_names:
                result += convert_single(_typ)
            return result
        else:
            return convert_single(type_names)


class Application(Object):
    """
    A kb object representing an application
    """
    def __init__(self, kb, identifier, name, typ):
        Object.__init__(self, kb, identifier, name, typ)

        self.projects_ids = []
        self.projects = []
        self.projects_by_id = {}
        self._calculate_project_list()
        
        # special job type for plugins. 
        # lifetime is not handled through castms.
        self.type_id = 141884
        # the job names created during the session
        self.job_names = set()
        self.current_plugin = None
        self.amt_saver = None
        
    def sql_tool(self, query):
        """
        Execute a so called SQL Tool.
        
        :param str or file query: sql batch

        @type query: str or file
        
        Basically it executes a query after having replaced some special variables :
        §
        
        See documentation of CAST-MS for further detail.
        """
        cursor = self.kb.create_cursor()
        
        
        self.kb._execute_raw_query(cursor, query)
        
        # we return result...
        return cursor

    def update_cast_knowledge_base(self, name, query):
        """
        Execute a so called Update Cast knowledge base.

        :param str or file query: sql batch

        @type query: str or file
        
        Basically it executes a query after having replaced some special variables :
        §
        
        to fill in CI_xxx tables and launch the tools that perform the update.
        
        See documentation of CAST-MS for further detail.
        """
        cursor = self.kb.create_cursor()
        
        plugin_name = self.current_plugin.get_name() if self.current_plugin else ''
        logging.debug('plugin name is %s', plugin_name)
        
        
        """
        plugin_name is generally of the form : a.b.c
        plugin_name is a directory name
        
        name can be anything
        """
        job_name = "%s/%s/%s" % (self.name, plugin_name, name)
        if job_name in self.job_names:
            raise RuntimeError('You cannot create 2 update_cast_knowledge_base with the same name : %s' % name)

        """
        @todo : C++ code does something before...
        """        
        self.job_names.add(job_name)
        
        # execute init
        job_id = self.create_job(job_name)
        
        is_mssql = False
        
        if self.kb.engine.dialect.name == 'mssql':
            is_mssql = True
            cursor.execute('exec ci_init_data %s' % job_id)
        else:
            self.kb._execute_function(cursor, 'ci_init_data', '%s' % job_id)
        
        self.kb.raw_connection.commit()
        
        # execute user query
        self.kb._execute_raw_query(cursor, query)

        # execute check and close 
        if is_mssql:
            cursor.execute('exec ci_check_data %s,%s' % (job_id, self.id))
        else:
            self.kb._execute_function(cursor, 'ci_check_data', '%s,%s' % (job_id, self.id))
        self.kb.raw_connection.commit()
        if is_mssql:
            cursor.execute('exec ci_process_data %s' % job_id)
        else:
            self.kb._execute_function(cursor, 'ci_process_data', '%s' % job_id)
        
        self.kb.raw_connection.commit()

    def get_knowledge_base(self):
        """
        Access to the knowledge base
        
        :rtype: :class:`cast.application.KnowledgeBase`

        """
        return self.kb
    
    def get_projects(self):
        """
        The result projects of the application.
        
        :rtype: iterable of :class:`cast.application.Project`
        """
        return self.projects
        
    def get_objects(self, internal=True, external=False):
        """
        Return a iterable collection of all the objects of the application.
        
        Projects are not considered as part of the of the application as they
        already are accessible as direct child through get_projects.

        :rtype: iterable of :class:`cast.application.Object`
        """
        return self.kb._get_objects(self.projects_ids, internal, external, self)

    def get_objects_by_name(self, name, internal=True, external=False):
        """
        Search objects of a given name 
        
        :param str name: name of the searched object.  
        
        :rtype: iterable of :class:`cast.application.Object`
        
        """
        return self.kb._get_objects_by_name(self.projects_ids, name, internal, external, self)

    def search_objects(self, name=None, category=None, load_properties=False):
        """
        Search objects by name or/and by type
        
        :param str name: the name of the searched object
        :param str category: the category name of the searched object
        :param bool load_properties: if True properties of objects will be available. Slower.
        
        :rtype: iterable of :class:`cast.application.Object`
        """
        return self.kb._search_objects(self.projects_ids, name, category, self, load_properties=load_properties)
    
    def get_files(self, languages=[]):
        """
        Get all the files of the application.
        
        :param list of str languages: possible categories for the searched files.  
        
        :rtype: iterable of :class:`cast.application.File`
        
        """
        return self.kb._get_files(self.projects_ids, languages, self)
        
    def get_databases(self, is_sorted=None):
        """
        Get all the database objects of the application.
        
        :rtype: iterable of :class:`cast.application.Database`
        
        """
        databases = []
        for project in self.projects:
            if not project:
                continue
            project.load_children(None, is_sorted)
            oracleInstanceFound = False
            try:
                # Oracle instances
                databases.extend(project.get_children(['CAST_Oracle_Instance']))
                if len(databases) > 0:
                    oracleInstanceFound = True
            except:
                pass
            try:
                # Legacy Oracle instances
                machines = list(project.get_children(['CAST_SQL_Machine']))
                for machine in machines:
                    machine.load_children(None, is_sorted)
                    databases.extend(machine.get_children(['CAST_SQL_Instance']))
            except:
                pass
            try:
                # Other instances
                instances = list(project.get_children(['CAST_SQL_InstanceContainer']))
                for instance in instances:
                    if instance.type.name != 'CAST_Oracle_Instance':
                        instance.load_children(None, is_sorted)
                        databases.extend(instance.get_children(['CAST_SQL_RootContainer']))
            except:
                pass
            try:
                # Legacy Other instances (DATABASE are projects)
                if project.type.name == 'DATABASE':
                    databases.append(project)
                if project.type.name == 'DB2ANALYZE':
                    databases.extend(project.get_children(['UDBDATABASE']))
                    
                
            except:
                pass

        return databases
        
    def declare_property_ownership(self, prop, type_names):
        """
        State that the current plugin handles the production of a property for some types.
        
        The current plugin will calculate all the values for that property for all the objects of those types.
        
        Necessary for saving properties.
        
        :param prop: the property to save. Either a string for the fullname of the property or an integer for the property id. 
        :param type_names: list of strings the names of the types or categories
        
        All given types must have the property.
        """
        _property = self._convert_into_property(prop)

        types = self._convert_into_types(type_names)
        
        def has_property(cat, prop):
    
            for c in cat.all_inherited_categories:
                if prop in c.get_properties():
                    return True
                
            return False

        for t in types:
            if not has_property(t, _property):
                raise RuntimeError("Property " + str(_property.get_name()) + " is not valid for type " + str(t.get_name()))
        
        self._get_raw_saver().declare_property(types, _property)
        
                
    def create_job(self, name, type_id=None):
        """
        Create or get an application level plugin job.
        Mark it as used. 
        See _mark_plugin_jobs_as_unused and _delete_unused_jobs.
        """
        if not type_id:
            type_id = self.type_id
        
        AnaJob = Table("anajob",
                       self.kb.metadata,
                       Column('idjob', Integer),
                       Column('jobnam', String(255)),
                       Column('jobtyp', Integer),
                       Column('jobver', Integer),
                       Column('idcnx', Integer),
                       Column('jobbegindate', TIMESTAMP),
                       Column('jobbegindate', TIMESTAMP),
                       autoload=True,
                       autoload_with=self.kb.engine,
                       extend_existing=True)
        
        UsrProJob = Table("usrprojob",
                          self.kb.metadata,
                          Column('idusrpro', Integer),
                          Column('idjob', Integer),
                          Column('ordnum', Integer),
                          Column('prop', Integer),
                          autoload=True,
                          autoload_with=self.kb.engine,
                          extend_existing=True)

        AnaAttr = Table("anaattr",
                       self.kb.metadata,
                       Column('session_id', Integer),
                       Column('attrnam', String(255)),
                       Column('intval', Integer),
                       autoload_with=self.kb.engine,
                       extend_existing=True)
        
        # search first if job of that name exist
        plugin_jobs = select([AnaJob.c.idjob]).where(AnaJob.c.jobtyp == type_id).where(AnaJob.c.jobnam == name)
        query = select([UsrProJob.c.idjob]).where(UsrProJob.c.idjob.in_(plugin_jobs)).where(UsrProJob.c.idusrpro == self.id)
        
        results = self.kb.engine.execute(query)
        job_id = None
        for result in results:
            job_id = result[0]
        
        
        # job exist : just marking its
        if job_id:
            
            # temp code
            delete = AnaAttr.delete().where(AnaAttr.c.session_id == job_id)
            self.kb.engine.execute(delete)
            
            ins = AnaAttr.insert().values(session_id=job_id,
                                          attrnam='SyncOnlyModified',
                                          intval=0)
            self.kb.engine.execute(ins)
            
            ins = AnaAttr.insert().values(session_id=job_id,
                                          attrnam='AnalysisInfoProperties_Common_EscalateLinks',
                                          intval=0)
            self.kb.engine.execute(ins)
            # temp code
            
            
            logging.debug('Recreating job name %s id %d ', name, job_id)
            self._mark_job_as_used(job_id)
            return job_id
        
        
        # job does not exist : creating
        dialect = self.kb.engine.dialect.name
        if dialect == 'mssql':
            
            """
            Next ID to allocate is in Parms table, so select then update.
            """
            
            Parms = Table("parms",
                          self.kb.metadata,
                          Column('lib', String(30)),
                          Column('intval', Integer),
                          autoload=True,
                          autoload_with=self.kb.engine,
                          extend_existing=True)
            
            logging.debug('getting id...')
            
            next_id = select([Parms.c.intval]).where(Parms.c.lib == 'Id')
            results = self.kb.engine.execute(next_id)
            for result in results:
                job_id = result[0]
            
            logging.debug('got id %d', job_id)
            
            logging.debug('reserving id...')
            self.kb.engine.execute(Parms.update().where(Parms.c.lib == 'Id').values(intval=job_id+1))
            logging.debug('reserved')
            
            logging.debug('executing insert Keys...')
            ins = self.kb.Keys.insert().values(idkey=job_id,
                                               keynam=name,
                                               objtyp=type_id,
                                               keytyp='XXXXXX',
                                               keysubtyp=-1,
                                               keyclass=-1,  # no need to have a correct keyclass
                                               keyprop=0,
                                               idusrdevpro='???',
                                               keydevdat=date.today())
            result = self.kb.engine.execute(ins)
            logging.debug('executed')
            
        else:
            ins = self.kb.Keys.insert().values(keynam=name,
                                               objtyp=type_id,
                                               keytyp='XXXXXX',
                                               keysubtyp=-1,
                                               keyclass=-1,  # no need to have a correct keyclass
                                               keyprop=0,
                                               idusrdevpro='???',
                                               keydevdat=date.today())
         
            result = self.kb.engine.execute(ins)
            job_id = result.inserted_primary_key[0]

        logging.debug('Creating new job name %s id %d ', name, job_id)
        
        ins = AnaJob.insert().values(idjob=job_id,
                                     jobnam=name,
                                     jobtyp=type_id,
                                     jobver=730,  # do not care in fact
                                     idcnx=-1)
        
        self.kb.engine.execute(ins)
        
        ins = UsrProJob.insert().values(idusrpro=self.id,
                                        idjob=job_id,
                                        ordnum=1,
                                        prop=0)
        # this makes the job seen as used
        self.kb.engine.execute(ins)

        # oracle need it        
        ins = AnaAttr.insert().values(session_id=job_id,
                                      attrnam='SyncOnlyModified',
                                      intval=0)
        self.kb.engine.execute(ins)
 
        # oracle need it        
        ins = AnaAttr.insert().values(session_id=job_id,
                                      attrnam='AnalysisInfoProperties_Common_EscalateLinks',
                                      intval=0)
        self.kb.engine.execute(ins)
       
        return job_id

    def _mark_plugin_jobs_as_unused(self):
        
        cursor = self.kb.create_cursor()
        cursor.execute("update usrprojob set prop = -1 where idusrpro = %d and idjob in (select idkey from keys where objtyp = %d)" % (self.id, self.type_id))
        self.kb.raw_connection.commit()
        
    def _mark_job_as_used(self, job_id):
        
        cursor = self.kb.create_cursor()
        cursor.execute("update usrprojob set prop = 0 where idusrpro = %d and idjob = %d" % (self.id, job_id))
        self.kb.raw_connection.commit()

    def _delete_unused_jobs(self):

        cursor = self.kb.create_cursor()

        cursor.execute("""
        delete from usrprojob
            where idusrpro = %d and
                  prop = -1 and
                  idjob in (select IdJob from anajob 
                             where JobTyp = %d)""" % (self.id, self.type_id))
        
        
        self.kb.raw_connection.commit()
        self.kb._execute_function(cursor, 'usrpro_cleanup')
        self.kb.raw_connection.commit()
        
    def _calculate_project_list(self):
        """
        Precalculate the list of result projects of that application.
        """
        cursor = self.kb.create_cursor()

        # first query with prodep : modern approach
        cursor.execute("select distinct(idpro) from prodep where idpromain in (select IdRoot from UsrProRoot where IdUsrPro = %s)" % self.id)
        p1 = set([x[0] for x in cursor])
        
        # second query with proroot : legacy. For example DB2 ZOS.
        cursor.execute("select distinct(IdPro) from ProRoot where IdRoot in(select IdRoot from UsrProRoot where IdUsrPro = %s)" % self.id)
        p2 = set([x[0] for x in cursor])
        
        # take both
        self.projects_ids = list(p1 | p2)

        self.projects = [self.kb._load_object(project) for project in self.projects_ids]
        
        for project in self.projects:
            if project:
                project.application = self
                
                self.projects_by_id[project.id] = project
        
    def _get_amt_saver(self):
        
        if not self.amt_saver:
            
            plugin_name = self.current_plugin.get_name() if self.current_plugin else ''
            logging.debug('plugin name is %s', plugin_name)
            """
            plugin_name is generally of the form : a.b.c
            plugin_name is a directory name
            
            name can be anything
            """
            job_name = '%s/%s' % (self.name, plugin_name)
            self.amt_saver = Saver(self, None, job_name, self.current_plugin.get_name(), self.current_plugin.get_version())
        
        return self.amt_saver

    def _run_amt_saver(self):
        """
        Finalisation phase called after current plugin execution
        """
        if self.amt_saver:
            
            # all saving at the end
            plugin_name = self.current_plugin.get_name() if self.current_plugin else ''
            job_name = '%s/%s' % (self.name, plugin_name)
            job_id = self.create_job(job_name)
            self.amt_saver.job_id = job_id
            
            self.amt_saver.save()
            # reset for next plugin : very important
            self.amt_saver = None

        if hasattr(self.current_plugin, "raw_saver") and self.current_plugin.raw_saver:
            self.current_plugin.raw_saver.save()
            # reset for next plugin : very important
            self.current_plugin.raw_saver = None
                

    def _get_raw_saver(self):
        
        # attach saver on plugin object
        if not hasattr(self.current_plugin, "raw_saver"):
            setattr(self.current_plugin, "raw_saver", None)
        
        if not self.current_plugin.raw_saver:
            saver = RawSaver(self)
            self.current_plugin.raw_saver = saver
        
        return self.current_plugin.raw_saver
            
    
    
    def _get_projects(self, o):
        
        cursor = self.kb.create_cursor()
        cursor.execute("select distinct(idpro) from ObjPro where idobj = %s" % o.id)
        return [self.projects_by_id[project_id[0]] for project_id in cursor]
        
    def __repr__(self):
        return 'Application(name=%s)' % (self.get_name())

    
class DatabaseSubset(Object):
    """
    A particular object that is a database subset
    """
    def __init__(self, kb, identifier, name, typ):
        Object.__init__(self, kb, identifier, name, typ)

    def __repr__(self):
        return 'DatabaseSubset(%s, %s)' % (self.get_name(), self.get_type())


class File(Object):
    """
    A kb object representing a source file.
    """
    def __init__(self, kb, identifier, name, typ, additional_values=None):
        Object.__init__(self, kb, identifier, name, typ, additional_values)
        self.path = None
        if additional_values and 'path' in additional_values:
            self.path = additional_values['path']
        
        # CAST_DotNet_File has no path, but fullname is path
        if not self.path and self.type and self.type.inherit_from(kb.metamodel.get_category(id=138870)):
            self.path = self.fullname
        
        # UA files, when using preprocessor, have path in temp and fullname is source path      
        if self.type and self.type.inherit_from(kb.metamodel.get_category(id=1000007)) and self.fullname:
            self.path = self.fullname
            
    def __repr__(self):
        return 'File(%s, %s)' % (self.get_name(), self.get_type())

    def get_path(self):
        """
        Get the source file path
        
        :rtype: str

        @rtype: str
        """
        
        # cached value
        if self.path:
            return self.path
        
        cursor = self.kb.create_cursor()
        cursor.execute("select Path from RefPath where IdFilRef in (select IdFilRef FROM ObjFilRef where IdObj=%s)" % self.id)

        line = cursor.fetchone()
        if line:
            self.path = line[0]

        return self.path

    def load_objects(self):
        
        if not self.sub_objects:
            self.sub_objects = []
            for o in self.kb._load_objects(self):
                self.sub_objects.append(o)
        
        return self.sub_objects


class DatabaseOwner(Object):
    """
    A kb object representing a database owner/Schema object.
    """
    def __init__(self, kb, identifier, name, typ, additional_values=None):
        Object.__init__(self, kb, identifier, name, typ, additional_values)
        
    def get_tables(self):
        
        self.load_children()
        return [ child for child in self.children if child.type.inherit_from('Database Table') ]
        
    def get_views(self):
         
        self.load_children()
        return [ child for child in self.children if child.type.inherit_from('Database View') ]
        
    def get_procedures(self):
         
        self.load_children()
        return [ child for child in self.children if child.type.inherit_from('Database Procedure') ]
        
    def get_functions(self):
         
        self.load_children()
        return [ child for child in self.children if child.type.inherit_from('Database Function') ]

    def __repr__(self):
        return 'Database Owner(%s, %s)' % (self.get_name(), self.get_type())
        

class Database(DatabaseOwner):
    """
    A kb object representing a database object.
    """
    def __init__(self, kb, identifier, name, typ, additional_values=None):
        DatabaseOwner.__init__(self, kb, identifier, name, typ, additional_values)
        self.legacy = False
        if typ.name in [ 'DATABASE', 'CAST_SQL_Instance' ]:
            self.legacy = True
        if typ.name in [ 'CAST_Oracle_Instance', 'CAST_SQL_Instance' ]:
            self.databaseType = 'ORACLE'
        else:
            self.databaseType = 'TSQL'
        
    def get_owners(self, is_sorted=False):
        """
        Get all the schemas of the database.
        
        :rtype: iterable of :class:`cast.application.DatabaseOwner`
        
        """ 
        self.load_children(None, is_sorted)
        
        owners = []
        try:
            owners.extend([ child for child in self.children if child.type.inherit_from('CAST_SQL_OwnerContainer') ])
            owners.extend([ child for child in self.children if child.type.inherit_from('SQL_SCHEMA') ])
            owners.extend([ child for child in self.children if child.type.inherit_from('CAST_SQL_Schema') ])
        except:
            try:
                owners.extend([ child for child in self.children if child.type.inherit_from('SQL_SCHEMA') ])
                owners.extend([ child for child in self.children if child.type.inherit_from('CAST_SQL_Schema') ])
            except:
                pass
        
        owners.extend([ child for child in self.children if child.type.inherit_from('UDBSCHEMA') ])
            
        return owners

    def __repr__(self):
        return 'Database(%s, %s)' % (self.get_name(), self.get_type())


class Project(Database):
    """
    A particular object that is the result of an Analysis Unit
    """
    def __init__(self, kb, identifier, name, typ, additional_values=None):
        Database.__init__(self, kb, identifier, name, typ, additional_values)

    def get_objects(self, internal=True, external=False):
        """
        Return a iterable collection of all the objects of the project
        """
        return self.kb._get_objects([self.id], internal, external)

    def __repr__(self):
        return 'Project(%s, %s)' % (self.get_name(), self.get_type())


Reference = collections.namedtuple('Reference', ['pattern_name',
                                                 'object',
                                                 'value',
                                                 'bookmark'])
"""
A reference found
"""


class ReferenceFinder:
    """
    Search for patterns in a text or file.
    """
    def __init__(self):
        self.token_specification = [
                ('NEWLINE', r'\n'),  # Line endings
                ('SKIP', r'[ \t]'),  # Skip over spaces and tabs
            ]

    def add_pattern(self, name, before, element, after):
        """
        Add a search pattern.
        
        :param str name: name of the pattern

        :param str element: a regular expression that is searched
        
        :param str before: a regular expression that should match before the searched element, may be empty but cannot be of variable length
       
        :param str after: a regular expression that should match after the searched element, may be empty
        
        So we search element preceded by before and followed directly by after.
        
        You may add several patterns. 
        First matching pattern will be recognised, so adding overlapping patterns is not recommended.
        
        """
        self.token_specification.append((name,
                                         '(?<=%s)%s(?=%s)' % (before,
                                                              element,
                                                              after)))

    def find_references_in_file(self, file):
        """
        Find references inside a file.
        
        :param file: either a file path or a File object
        
        :rtype: iterable of :class:`cast.application.Reference`
        """
        path = None
        file_object = None
        if type(file) is str:
            path = file
        else:
            path = file.get_path()
            file_object = file
        
        try:
            content = self.read(path)
            return self._find_references(content, file_object)
        except FileNotFoundError:
            return []

    def read(self, path):
        
        try:
            with open(path, 'r', encoding='UTF-8') as f:
                return f.read()
        except:
            with open(path, 'r') as f:
                return f.read()
                
    def _find_references(self, string, file_object=None):
        """
        Returns all references found as an iterable.
        
        :param iterable of Reference
        """
        
        tok_regex = '|'.join('(?P<%s>%s)' % pair for pair in self.token_specification)
        # search instead of match to cope with non expressed elements...
        get_token = re.compile(tok_regex).search
        line = 1
        
        # line_start is the position in file of the last begin line
        pos = line_start = 0
        mo = get_token(string)
        while mo is not None:
            typ = mo.lastgroup
            pos = mo.end()
            if typ == 'NEWLINE':
                line_start = pos
                line += 1
            elif typ != 'SKIP':
                val = mo.group(typ)
                
                column = mo.start() - line_start + 1
                
                end_column = column + len(val)
                
                # number of new lines 
                newlines = val.count('\n')
                
                last_newline = val.rfind('\n')
                
                if last_newline != -1:
                    end_column = len(val) - last_newline - 1
                
                yield Reference(typ,
                                file_object if not file_object else file_object.find_most_specific_object(line, column),
                                val,
                                Bookmark(file_object, line, column, line + newlines, end_column)) 
                
                # @todo use 
                line += newlines
                line_start = pos - end_column
                
            mo = get_token(string, pos)



def replace_special_variables(query):
    """
    Replaces inside a text :
        §

    """
    
    # strip comments
    query = sqlparse.format(query, strip_comments=True)
    
    # check for presence of old non standard things...
    if any(x in query for x in ['§']):
        
        query = query.replace('§', '')
        
        logging.warning('you are using a deprecated feature. You can avoid this warning by replacing your query by the following one : %s', query)
        
    return query


def _remove_last_comma(statement):
    
    for i in range(len(statement) - 1, -1, -1):
        c = statement[i]
        if c != ' ' and c != '\t' and c != '\n' and c != '\r':
            if c == ';':
                return statement[:i]
            else:
                return statement[:i + 1]



class Saver:
    """
    Saver using AMT saving.
    """
    def __init__(self, application, job_id, name, plugin_id, plugin_version):
        
        self.plugin_id = plugin_id
        self.plugin_version = plugin_version
        self.name = name
        
        self.user_project_id = application.id
        self.kb = application.kb
        self.engine = self.kb.engine
        self.metadata = self.kb.metadata
        self.job_id = job_id
        self.next_id = 1
        self.property_char_offset = 0
        self.property_int_offset = 0

        self.project_type = self.kb.plugin_project_type
        
        self.in_project_link = 1054
        self.parent_link = 1032
        
        self.IN_OBJECTS = Table("in_objects", 
                                self.metadata, 
                                Column('session_id', Integer),
                                Column('object_id', Integer),
                                Column('name_id', String(1015)),
                                Column('short_name_id', String(600)),
                                Column('object_type_id', Integer),
                                autoload_with=self.engine,
                                keep_existing=True)
         
        self.IN_LINKS = Table("in_links",  
                              self.metadata, 
                              Column('session_id', Integer),
                              Column('link_id', Integer),
                              Column('source_id', Integer),
                              Column('target_id', Integer),
                              Column('project_id', Integer),
                              Column('source_kind', String(1)),
                              Column('target_kind', String(1)),
                              Column('project_kind', String(1)),
                              Column('link_type_id', Integer),
                              autoload_with=self.engine,
                              keep_existing=True)
          
        self.IN_CHAR_PROPERTIES = Table("in_char_properties", 
                                        self.metadata, 
                                        Column('session_id', Integer),
                                        Column('object_id', Integer),
                                        Column('property_type_id', Integer),
                                        Column('property_offset', Integer),
                                        Column('char_block', Integer),
                                        Column('property_char', String(255)),
                                        autoload_with=self.engine,
                                        keep_existing=True)
        
        self.IN_INT_PROPERTIES = Table("in_int_properties", 
                                       self.metadata, 
                                       Column('session_id', Integer),
                                       Column('object_id', Integer),
                                       Column('property_type_id', Integer),
                                       Column('property_offset', Integer),
                                       Column('property_int', Integer),
                                       autoload_with=self.engine,
                                       keep_existing=True)
        
        self.IN_POSITIONS = Table("in_positions", 
                                  self.metadata, 
                                  Column('session_id', Integer),
                                  Column('object_id', Integer),
                                  Column('object_source_id', Integer),
                                  Column('object_source_kind', String(1)),
                                  Column('seq_num', Integer),
                                  Column('position_mode', Integer),
                                  Column('position1', Integer),
                                  Column('position2', Integer),
                                  Column('position3', Integer),
                                  Column('position4', Integer),
                                  Column('group_num', Integer),
                                  autoload_with=self.engine,
                                  keep_existing=True)

        self.cursor = self.kb.create_cursor()
        self.raw_connection = self.kb.raw_connection

        # saving cache : fill this and fill in tables latter
        self.in_objects = []
        self.in_links = []
        self.in_int_properties = []
        self.in_char_properties = []
        self.in_positions = []
        
        self._create_project(self.name)
        
        # projects on wich this one depend
        self.dependent_projects = set()
        
        


    def add_link(self, link_type, caller, callee, bookmark=None):
        """
        Create a link between 2 objects.
        
        @param link_type: either an integer, a string or a Type
        @param caller: either an integer or an Object
        @param callee: either an integer or an Object
        """
        link_id = self.next_id
        self.next_id += 1

        AMT_id_type = 'E'
        KB_id_type = 'I'

        link_type_id = None
        if type(link_type) is int:
            link_type_id = link_type
        elif type(link_type) is str:
            link_type_id = self.kb.metamodel.get_category(name=link_type).id
        else:
            link_type_id = link_type.id
        
        self.in_links.append([self.job_id,
                              link_id,
                              caller if type(caller) is int else caller.id,
                              callee if type(callee) is int else callee.id,
                              -1 if link_type_id == self.in_project_link else self.project_id, 
                              AMT_id_type if type(caller) is int else KB_id_type,
                              AMT_id_type if type(callee) is int else KB_id_type,
                              AMT_id_type,
                              link_type_id
                              ])
        
        
        if bookmark:
            self.add_link_bookmark(link_id, bookmark)
        
        return link_id

    def add_link_bookmark(self, link, bookmark):
        """
        Save a bookmark on a link.
        """
        
        self.in_positions.append([self.job_id,
                                  link,
                                  bookmark.file.id,
                                  'I',
                                  0,  # sequence number are computed in base for links
                                  2,  # line/col position type
                                  bookmark.begin_line,
                                  bookmark.begin_column,
                                  bookmark.end_line,
                                  bookmark.end_column,
                                  -1
                                  ])
        
    def add_property(self, object, property, value):
        
        if property.get_type() == 'string' and type(value) is str:

            # @ todo : should iterate and split on valid utf8 strings maxed out at 255 bytes and increase blkno
            self.in_char_properties.append([self.job_id,
                                            object if type(object) is int else object.id,
                                            property.id,
                                            self.property_char_offset,
                                            0,
                                            value
                                            ])
            
            self.property_char_offset += 1

        elif property.get_type() == 'integer' and type(value) is int:
            
            self.in_int_properties.append([self.job_id,
                                           object if type(object) is int else object.id,
                                           property.id,
                                           self.property_int_offset,
                                           value
                                           ])

            self.property_int_offset += 1

    def save(self):
        """
        Really save.
        """
        
        self._empty_in_tables()
        
        logging.debug('executing cache_init %d ...', self.job_id)
        self.kb._execute_function(self.cursor, 'cache_init', '%s' % self.job_id)
        self.raw_connection.commit()
        logging.debug('executed')

        
        # add all the dependency to other projects
        for project in self.dependent_projects:
            link_id = self.add_link(1056, self.project_id, project)
            self.add_property(link_id, self.kb.dependencyKind, 0)
            
        self._display_in_tables()
        
        # flush IN tables

        for val in self.in_links:
            val[0] = self.job_id
        ins = self.IN_LINKS.insert()
        self.cursor.executemany(str(ins.compile()), self.in_links)
        self.raw_connection.commit()
        self.in_links = []
        

        for val in self.in_objects:
            val[0] = self.job_id
        ins = self.IN_OBJECTS.insert()
        self.cursor.executemany(str(ins.compile()), self.in_objects)
        self.raw_connection.commit()
        self.in_objects = []

        for val in self.in_positions:
            val[0] = self.job_id
        ins = self.ins = self.IN_POSITIONS.insert()
        self.cursor.executemany(str(ins.compile()), self.in_positions)
        self.raw_connection.commit()
        self.in_positions = []

        for val in self.in_char_properties:
            val[0] = self.job_id
        ins = self.IN_CHAR_PROPERTIES.insert()
        self.cursor.executemany(str(ins.compile()), self.in_char_properties)
        self.raw_connection.commit()
        self.in_char_properties = []
        
        for val in self.in_int_properties:
            val[0] = self.job_id
        ins = self.IN_INT_PROPERTIES.insert()
        self.cursor.executemany(str(ins.compile()), self.in_int_properties)
        self.raw_connection.commit()
        self.in_int_properties = []
        
        logging.debug('executing cache_processid %d, %d ...', self.job_id, self.user_project_id)
        self.kb._execute_function(self.cursor, 'cache_processid', '%s,%s' % (self.job_id, self.user_project_id))
        self.raw_connection.commit()
        logging.debug('executed')
        logging.debug('executing cache_flushdata %d', self.job_id)
        self.kb._execute_function(self.cursor, 'cache_flushdata', '%s' % self.job_id)
        self.raw_connection.commit()
        logging.debug('executed')

    def _create_project(self, name):
        """
        Creates the result project for that saving session.
        """
        self.project_id = self._create_object(name, self.project_type)
        
        link_id = self.add_link(1054, self.project_id, self.project_id)
        self.add_property(link_id, self.kb.projectRelationKind, 0)

        self.add_property(self.project_id, self.kb.identification_name, name)
        self.add_property(self.project_id, self.kb.identification_fullname, name)
        
        try:
            id_property = self.kb.metamodel.get_property(id=140567)
            version_property = self.kb.metamodel.get_property(id=140568)
            
            self.add_property(self.project_id, id_property, self.plugin_id)
            self.add_property(self.project_id, version_property, self.plugin_version)
            
        except KeyError:
            # before 7.3.6
            pass
        
        
    def _create_object(self, guid, object_type):
        short_guid = guid[0:600]
        
        long_guid = guid
        if len(long_guid) > 1000:
            crc = binascii.crc32(long_guid)
            long_guid = '%s <#%08X>' % (long_guid[0:1000], crc)
        
        object_id = self.next_id
        self.next_id += 1
        
        self.in_objects.append([self.job_id,
                                object_id,
                                long_guid, # so called guid
                                short_guid, # guid but with max 600
                                object_type if type(object_type) is int else object_type.id
                                ])
        return object_id
    
    def _empty_in_tables(self):
        
        ins = self.IN_OBJECTS.delete().where(self.IN_OBJECTS.c.session_id == self.job_id)
        self.engine.execute(ins)
        
        ins = self.IN_LINKS.delete().where(self.IN_LINKS.c.session_id == self.job_id)
        self.engine.execute(ins)
        
        ins = self.IN_CHAR_PROPERTIES.delete().where(self.IN_CHAR_PROPERTIES.c.session_id == self.job_id)
        self.engine.execute(ins)

        ins = self.IN_INT_PROPERTIES.delete().where(self.IN_INT_PROPERTIES.c.session_id == self.job_id)
        self.engine.execute(ins)
    
    def _display_in_tables(self):
        
        logging.debug('IN_OBJECTS :')
        self.cursor.execute("select * from IN_OBJECTS")
        self._print_result()

        logging.debug('IN_LINKS :')
        self.cursor.execute("select * from IN_LINKS")
        self._print_result()

        logging.debug('IN_CHAR_PROPERTIES :')
        self.cursor.execute("select * from IN_CHAR_PROPERTIES")
        self._print_result()

        logging.debug('IN_INT_PROPERTIES :')
        self.cursor.execute("select * from IN_INT_PROPERTIES")
        self._print_result()

    def _print_result(self):
        for line in self.cursor:
            logging.debug('%s', str(line))
        
    def _add_dependency(self, o):
        """
        Add a dependency to the object's project
        """
        if type(o) is int:
            return
        
        for project in o.get_projects():
            self.dependent_projects.add(project)


# see : http://stackoverflow.com/questions/6043463/split-unicode-string-into-300-byte-chunks-without-destroying-characters
def split_utf8(s, n):
    """Split UTF-8 s into chunks of maximum length n."""
    while len(s) > n:
        k = n
        while (ord(s[k]) & 0xc0) == 0x80:
            k -= 1
        yield s[:k]
        s = s[k:]
    yield s
    

class RawSaver:
    """
    A raw saver for thing unhandled by AMT saving 
    """
    def __init__(self, application):
        
        self.application = application
        self.kb = application.kb
        self.kb._load_infsub_types()
        
        self.ObjPro = application.kb.ObjPro
        self.Keys = application.kb.Keys
        
        self.ObjDsc = Table("objdsc", 
                            self.kb.metadata, 
                            Column('idobj', Integer, primary_key=True),
                            Column('inftyp', Integer, primary_key=True),
                            Column('infsubtyp', Integer, primary_key=True),
                            Column('blkno', Integer, primary_key=True),
                            Column('ordnum', Integer, primary_key=True),
                            Column('prop', Integer),
                            Column('infval', String(255)),
                            autoload=True, 
                            autoload_with=self.kb.engine,
                            keep_existing=True)
        
        self.ObjInf = Table("objinf", 
                            self.kb.metadata, 
                            Column('idobj', Integer),
                            Column('inftyp', Integer),
                            Column('infsubtyp', Integer),
                            Column('blkno', Integer),
                            Column('infval', Integer),
                            autoload=True, 
                            autoload_with=self.kb.engine,
                            keep_existing=True)
        
        self.Violations = Table("dss_positions",
                                self.kb.metadata,
                                Column('metricpositionid', Integer),
                                Column('objectid', Integer),
                                Column('propertyid', Integer),
                                Column('sourceid', Integer),
                                Column('positionid', Integer),
                                Column('positionindex', Integer),
                                Column('linestart', Integer),
                                Column('colstart', Integer),
                                Column('lineend', Integer),
                                Column('colend', Integer),
                                autoload=True, 
                                autoload_with=self.kb.engine,
                                keep_existing=True)
        
        self.possessions = []
        self.properties = []
        self.violations = []

    
    def declare_property(self, types, _property):
        """
        Declare possession of a property on some object types
        
        types is a list of types
        property must be a property with inftyp/infsubtyp
        
        """
        if not hasattr(_property,'inftyp') or not hasattr(_property,'infsubtyp'):
            raise RuntimeError('Cannot declare the possession of a property that do not have inftyp/infsubtyp')
        
        self.possessions.append((types, _property))

    
    def add_property(self, _object, _property, value):
        if not self._check(_object, _property):
            return
        
        if _property.get_type() == 'string':
            if type(value) is str:
                pass
            elif type(value) is list:
                if len(value):
                    sample = value[0]
                    if not type(sample) is str:
                        raise RuntimeError('Incorrect value type for save_property : property is of string type')
            else:
                raise RuntimeError('Incorrect value type for save_property : property is of string type')
        elif _property.get_type() == 'integer':
            if type(value) is int:
                pass
            elif type(value) is list:
                if len(value):
                    sample = value[0]
                    if not type(sample) is int:
                        raise RuntimeError('Incorrect value type for save_property : property is of integer type')
            else:
                raise RuntimeError('Incorrect value type for save_property : property is of integer type')
        else:
            raise RuntimeError('Property should be integer or string type')
        
        self.properties.append((_object.id, _property, value))
    
    def add_violation(self, _object, _property, bookmark, additional_bookmarks=[]):
        if not self._check(_object, _property):
            return

        if _property.get_type() != 'integer':
            raise RuntimeError('Property should be integer type for a violation')
        
        self.violations.append((_object.id, _property, bookmark, additional_bookmarks))

            
    def save(self):
        
        # 1. clean all possessed properties and violations
        self._clean()
        # warning : order counts
        self._save_violations()
        self._save_properties()
        
        # et voila!

    def _clean(self):
        
        # cleanup phase
        for possession in self.possessions:
            
            # query object in project and type is the current possession
            is_in_project = select([self.ObjPro.c.idobj]).where(self.ObjPro.c.idpro.in_(self.application.projects_ids))
            is_in_project = is_in_project.where(self.ObjPro.c.prop == 0)

            query = select([self.Keys.c.idkey])
            query = query.where(self.Keys.c.idkey.in_(is_in_project))
            query = query.where(self.Keys.c.objtyp.in_([t.id for t in possession[0]]))
            
            prop = possession[1]
            
            inftyp = prop.inftyp
            infsubtyp = prop.infsubtyp
            
            if prop.get_type() == 'string':
                # delete from objdsc
                d = delete(self.ObjDsc).where(self.ObjDsc.c.inftyp == inftyp).where(self.ObjDsc.c.infsubtyp == infsubtyp).where(self.ObjDsc.c.idobj.in_(query))
                self.kb.engine.execute(d)
            
            elif prop.get_type() == 'integer':
                # delete from objinf
                d = delete(self.ObjInf).where(self.ObjInf.c.inftyp == inftyp).where(self.ObjInf.c.infsubtyp == infsubtyp).where(self.ObjInf.c.idobj.in_(query))
                self.kb.engine.execute(d)

            # delete from violations
            d = delete(self.Violations).where(self.Violations.c.objectid.in_(query)).where(self.Violations.c.propertyid == prop.id)
            self.kb.engine.execute(d)
    
    
    def _get_values_and_violations(self):
        
        # stores object -> property -> integer : the violation count
        property_values = defaultdict(dict)
                
        # object -> property -> list of (bookmark, additional_bookmarks)
        ordered_violations = defaultdict(dict)
        
        # order and count violations
        for violation in self.violations:
            object_id, _property, bookmark, additional_bookmarks = violation
            
            temp = ordered_violations[object_id]
            
            if _property in temp:
                temp[_property].append((bookmark, additional_bookmarks))
            else:
                temp[_property] = [(bookmark, additional_bookmarks)]

            temp = property_values[object_id]
            if _property in temp:
                temp[_property] += 1
            else:
                temp[_property] = 1
                
        return property_values, ordered_violations
    
    def _save_violations(self):
        # 2. insert violations 
        
        # get the next violation id
        query = select([func.max(self.Violations.c.metricpositionid)])
        result = self.kb.engine.execute(query)
        max_metric_position_id = result.fetchone()[0]
        if not max_metric_position_id:
            max_metric_position_id = 0
        
        metric_position_id = max_metric_position_id+1  
        
        violation_content = []
        
        property_values, ordered_violations = self._get_values_and_violations()
        
        
        # dss_positions works the following way : 
        # for each couple (objectid, propertyid) we have a unique metric_position_id
        # then we must have a reltively unique positionid to distinguish the distinct violation patterns for the same (objectid, propertyid) 
        # then we must have distinct position_index for the several bookmarks of a same violation pattern

        # paranoid : we change positionid every line
        positionid = 1
        
        # scan violation but following object, prop order
        for object_id in ordered_violations.keys():
            
            t1 = ordered_violations[object_id]
            for _property in t1.keys():
                # list of couple bookmark, additional_bookmarks
                vs = t1[_property]
                
                for bookmark, additional_bookmarks in vs:
                    position_index = 1 # starts at one 
                    
                    violation_content.append((metric_position_id, # == unique per (objectid, property)
                                              object_id,
                                              _property.id,
                                              bookmark.file.id,
                                              positionid, 
                                              position_index,
                                              bookmark.begin_line,
                                              bookmark.begin_column,
                                              bookmark.end_line,
                                              bookmark.end_column
                                              ))
                    
                    for additional in additional_bookmarks:
                        position_index += 1 # same violation next bookmark
                        violation_content.append((metric_position_id,
                                                  object_id,
                                                  _property.id,
                                                  additional.file.id,
                                                  positionid, 
                                                  position_index,
                                                  additional.begin_line,
                                                  additional.begin_column,
                                                  additional.end_line,
                                                  additional.end_column
                                                  ))
                
                    positionid += 1 # increased for each violation
                
                metric_position_id += 1 # increased for each property
                
            metric_position_id += 1 # and object



        if violation_content:
            ins = self.Violations.insert().values(violation_content)
            self.kb.engine.execute(ins)
        
        # automatically fill integer properties to be setted with the violation count
        for objectid in property_values:
            for prop in property_values[objectid]:
                self.properties.append((objectid, prop, property_values[objectid][prop])) 
    
    def _save_properties(self):
        # 3. insert the values for properties
        integer_values = []
        string_values = []
        for object_id, prop, value in self.properties:
            
            # handle list/non list 
            local_values = value
            if not type(value) is list:
                local_values = [value]
            
            if prop.get_type() == 'integer':
                
                block_number = 0 
                for elementary_value in local_values:
                    integer_values.append((object_id,
                                           prop.inftyp,
                                           prop.infsubtyp,
                                           block_number,
                                           elementary_value))
                    block_number += 1
                
            elif prop.get_type() == 'string':
                
                block_number = 0 
                for elementary_value in local_values:
                    for storable_value in split_utf8(elementary_value, 255):
                    
                        string_values.append((object_id,
                                              prop.inftyp,
                                              prop.infsubtyp,
                                              block_number, 
                                              0, # 
                                              0,
                                              storable_value))
                        block_number += 1
                    block_number += 1
        
        if string_values:
            ins = self.ObjDsc.insert().values(string_values)
            self.kb.engine.execute(ins)

        if integer_values:
            ins = self.ObjInf.insert().values(integer_values)
            self.kb.engine.execute(ins)


    def _check(self, _object, _property):
        """
        Check that _object is allowed for _property
        """
        for possession in self.possessions:
            if possession[1] == _property and _object.type in possession[0]:
                return True
        
        raise RuntimeError('Property %s was not declared as hanlded for that type %s' % (str(_property), str(_object.type)))
        return False
    

####################
## install part
####################
import sys
import cast.application
import inspect
from distutils.version import StrictVersion

cast_module = sys.modules["cast.application"]
clsmembers = inspect.getmembers(cast_module, inspect.isclass)


def get_version(cast_module):
    
    if hasattr(cast_module, '__version__'):
        return getattr(cast_module, '__version__')
    else:
        return '1.0.0'

# list of classes that are part of this patch
patch_classes_names = {'Application':Application, 
                       'Bookmark':Bookmark, 
                       'Database':Database, 
                       'DatabaseOwner':DatabaseOwner, 
                       'DatabaseSubset':DatabaseSubset, 
                       'File':File, 
                       'KnowledgeBase':KnowledgeBase, 
                       'Object':Object, 
                       'Project':Project, 
                       'Reference':Reference, 
                       'ReferenceFinder':ReferenceFinder}


if StrictVersion(get_version(cast_module)) < StrictVersion('1.4.5'):
    """
    lower version so we install ourselves
    """
    setattr(cast_module, '__version__', '1.4.5')
    
    patched_classes = []
    
    for cast_class in clsmembers:
        
        class_name = cast_class[0]
        class_object = cast_class[1]
        
        if class_name in patch_classes_names:
            
            patched_classes.append(class_name)
            
            patch_class=patch_classes_names[class_name]
            
            for m in inspect.getmembers(patch_class):
                
                member_name = m[0]
                
                if not member_name.startswith('__') or member_name.startswith('__init') or member_name in ['__repr__', '__eq__', '__hash__']:
                    setattr(class_object, member_name, m[1])
    
    # remaining classes : added to module
    for name, cls in patch_classes_names.items():
        
        if not name in patched_classes:
            
            setattr(cast_module, name, cls)
    
    # non generic
    # for > 8.0.0
    try:
        from cast.application.internal import get_current_application
        # reload projects
        get_current_application()._calculate_project_list()
    except:
        pass
        
    