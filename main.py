import cast_upgrades.cast_upgrade_1_5_0 # @UnusedImport

from cast.application import ApplicationLevelExtension, ReferenceFinder, create_link
import logging
import re

class MyLiferayExtension(ApplicationLevelExtension):

    def __init__(self):   
        pass     
        
    def end_application(self, application):

        self.global_nb_links = 0 
        self.links_through_ActionMapping_SpringMVC(application)
        self.links_through_RequestMapping_SpringMVC(application)
        self.links_to_error_pages(application)
        logging.info("Nb of links created globally " + str(self.global_nb_links)) 


    def links_through_ActionMapping_SpringMVC(self, application): 
        
        nb_links = 0 
        logging.info("==> Solving the following problem : Missing links between Portlet (JSP) and Java Methods")    

        # 1. search all references in all files
 
        logging.info("Scanning Portlets for calls to Java Methods through Annotation ActionMapping (Spring MVC)")
            
        Java_Methods = {}     
        
        for Java_Method in application.search_objects(category='JV_METHOD', load_properties=True):
            #logging.debug("Java Method [" + str(Java_Method) + "]")
            for Annotation in Java_Method.get_property("CAST_Java_AnnotationMetrics.Annotation"): 

                if str(Annotation).startswith("@ActionMapping"): 
                    #logging.debug("Java Method get Property Annotation StartsWith[" + Annotation + "]")
                    if '"' in str(Annotation):
                        Target = str(Annotation).split("\"")[1]
                        #logging.debug("Java Method [" + str(Java_Method) + "] Target[" + Target + "]")
                        Java_Methods[Target] = Java_Method 
            
        # 2. scan each JSP file 
        # we search a pattern
        portlet_and_method_access = ReferenceFinder()
        #portlet_and_method_access.add_pattern("ActionMapping", before="", element="<portlet:actionURL name=\"[a-zA-Z0-9_-]+", after="")
        portlet_and_method_access.add_pattern("ActionMapping", before="<portlet:actionURL", element="[A-Za-z0-9\=_\-\" ]+", after="")

 
        links = []
                 
        for o in application.get_files(['CAST_Web_File']): 

            # check if file is analyzed source code, or if it generated (Unknown)
            if not o.get_path():
                continue
            
            for reference in portlet_and_method_access.find_references_in_file(o):
                #logging.debug("Reference " + reference.value)      
                
                # manipulate the reference pattern found
                if not 'name=\"' in reference.value:
                    continue
                searched_java_method_name = reference.value.split("\"")[1]
                #logging.debug("searching " + searched_java_method_name)
 
                try:
                    Java_Method = Java_Methods[searched_java_method_name]
                    links.append(('callLink', reference.object, Java_Method, reference.bookmark))
                    
                except:
                    pass
         
        # 3. Create the links
        for link in links:
            logging.debug("Creating link between " + str(link[1]) + " and " + str(link[2]))
            create_link(*link)
            nb_links = nb_links + 1 
                
        logging.debug("Nb of links created " + str(nb_links))
        self.global_nb_links = self.global_nb_links + nb_links              
 

    def links_through_RequestMapping_SpringMVC(self, application): 
        
        nb_links = 0 
        logging.info("==> Solving the following problem : Missing links between Java Methods and Portlet (JSP) response ")    

        # 1. search all references in all files
 
        logging.info("Scanning Java Methods for calls to Portlet through Annotation RequestMapping (Spring MVC)")
            
        Java_Methods = {}  
        Request_Mappings = {}   
        
        # annotation RequestMapping
        request_mapping = next(application.get_objects_by_name(name="RequestMapping", external=True))
        #logging.debug("Annotation request Mapping [" + str(request_mapping) + "]")
        
        # all method with a link to annotation 
        for link in application.links().has_callee([request_mapping]).has_caller(application.objects().is_executable()).load_positions():
    
            JSPRedirection = False  
            
            #logging.debug("caller (Method calling the RequestMapping Annotation) [" + str(link.get_caller()) + "]")
            
            Java_Methods = application.search_objects(name=link.get_caller().get_name(), load_properties=True) 
            for Java_Method in Java_Methods:            
                #logging.debug("Java Method =[" + str(Java_Method) + "]") 

                for Annotation in Java_Method.get_property("CAST_Java_AnnotationMetrics.Annotation"): 
    
                    if str(Annotation).startswith("@RequestMapping(params=\"action="): 
                        AnnotationAction = str(Annotation).split("\"")[1].split("=")[1]
                        #logging.debug("Annotation Action =[" + AnnotationAction +"]")

            # caller code :
            code = link.get_caller().get_positions()[0].get_code()
            #logging.debug(code)
            #Search in code the following 
            #return \"[A-Za-z0-9_-]+\" 
            
            #JSP with same name can be part of different packages 
            main_package = link.get_caller().get_fullname().split(".")[4] 
            #logging.debug("Main package [" + main_package + "] + caller name = [" + link.get_caller().get_fullname() + "]")
            
            # Return all words beginning with character 'a', as an iterator yielding match objects
            it = re.finditer("return \"[A-Za-z0-9_\-\/]+\"", code)
            for match in it:
                #logging.debug("match = [" + format(match.group()) + "]")
                if "/" in format(match.group()): 
                    redirectJSP = format(match.group()).split("/")[1].split("\"")[0] + ".jsp"
                if "/" not in format(match.group()):
                    redirectJSP = format(match.group()).split("\"")[1] + ".jsp"
                #logging.debug("Redirect JSP [" + redirectJSP + "]") 
                redirectJSP_iter = application.get_objects_by_name(name=redirectJSP) 
                for redirectJSP_object in redirectJSP_iter:
                    #logging.debug("Redirect JSP [" + redirectJSP + "] + [" + str(redirectJSP_object) + "]")
                    #logging.debug("Redirect JSP full name[" + redirectJSP_object.get_fullname() + "]")
                    if main_package in redirectJSP_object.get_fullname(): 
                        #logging.debug("Main package found for JSP [" + main_package +"]")
                        Request_Mappings[str(main_package + AnnotationAction)] = redirectJSP_object 
                        JSPRedirection = True
                
            if JSPRedirection == False:
                #logging.debug(code)
                pass
                
        # 2. scan each Java method 
        # we search a pattern
        request_mapping_access = ReferenceFinder()
        request_mapping_access.add_pattern("RequestMapping", before="", element="response\.setRenderParameter\(\"action\",[ ]+\"[a-zA-Z0-9_-]+", after="")
 
        links = []
                 
        for o in application.get_files(['JV_FILE']): 

            # check if file is analyzed source code, or if it generated (Unknown)
            if not o.get_path():
                continue
            
            for reference in request_mapping_access.find_references_in_file(o):
                #logging.debug("Reference " + reference.value)      
                
                # manipulate the reference pattern found
                searched_request_mapping = reference.value.split("\"")[3]
                #logging.debug("searching searched_request_mapping [" + searched_request_mapping + "]")
                
                searched_package = o.get_path().split("\\src\\")[1].split("\\")[0] 
                #logging.debug("o.get_path() [" + o.get_path() + "]")
                #logging.debug("searched package [" + searched_package + "]") 
                 
                try:
                    JSP_Redirect = Request_Mappings[str(searched_package + searched_request_mapping)]
                    links.append(('callLink', reference.object, JSP_Redirect, reference.bookmark))
                    pass 
                    
                except:
                    pass
         
        # 3. Create the links
        for link in links:
            logging.debug("Creating link between " + str(link[1]) + " and " + str(link[2]))
            create_link(*link)
            nb_links = nb_links + 1 
                
        logging.debug("Nb of links created " + str(nb_links))        
        self.global_nb_links = self.global_nb_links + nb_links              
  


    def links_to_error_pages(self, application): 
        
        nb_links = 0 
        logging.info("==> Solving the following problem : Missing links to error or exception pages ")    
            
        links = []
        
        error_JSP = "applicationErrorView.jsp"
        error_JSP_iter = application.get_objects_by_name(name=error_JSP) 
        for error_JSP_object in error_JSP_iter:
            #logging.debug("Error JSP full name[" + error_JSP_object.get_fullname() + "]")
            
            java_exceptions = {"fi.op.jopo.exception.JopoException", 
                               "fi.op.jopo.exception.JopoApplicationException", 
                               "fi.op.jopo.exception.JopoSystemException"}
            for java_exception in java_exceptions: 
                java_exception_short_name = java_exception.split("exception.")[1] 
                #logging.debug("Java Exception short name[" + java_exception_short_name + "]")
                java_exception_iter = application.get_objects_by_name(name=java_exception_short_name, external=True)
                for java_exception_object in java_exception_iter:
                    if (java_exception_object.get_fullname() == java_exception):
                        #logging.debug("Java Exception full name[" + java_exception_object.get_fullname() + "]")
                        links.append(('callLink', java_exception_object, error_JSP_object, 0))

        # 3. Create the links
        for link in links:
            logging.debug("Creating link between " + str(link[1]) + " and " + str(link[2]))
            create_link(*link)
            nb_links = nb_links + 1 
                
        logging.debug("Nb of links to error pages created " + str(nb_links)) 
        self.global_nb_links = self.global_nb_links + nb_links              
         
