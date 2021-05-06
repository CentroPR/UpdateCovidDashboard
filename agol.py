import pandas as pd
from arcgis.gis import GIS,Item
from arcgis import features
from copy import deepcopy

class ItemUpdate:
    def __init__(self,gis,itemid,itemdict=None,userdict=None):
        """Does not acount for cases where their are more than one search result"""
 
        self.item=Item(gis,itemid,itemdict=itemdict)
        if self.item.layers:
            self.lyr=self.item.layers[0]
            self.properties=self.lyr.properties
            self.fields=self.properties.fields
        
#        query=' '.join("%s:%r" % (key,val) for (key,val) in kwargs.items())
#        if "typ" in query:
#            query=query.replace("typ","type").replace("'","")
#
#        item=gis.content.search(query)
#        assert item !=[],'The query provided did not return an item.'
#        self.item  =item[0]

    def overwriteItem(self,new_item):
        from arcgis.features import FeatureLayerCollection
        layer_collection = FeatureLayerCollection.fromitem(self.item)

        #call the overwrite() method which can be accessed using the manager property
        layer_collection.manager.overwrite(new_item)   

    def add_field(self,field_props=dict, **kwargs):
        field_properties=["name",
                          "alias",
                          "domain",
                          "editable",
                          "nullable",
                          "required",
                          "length",
                          "type",
                          "sqlType",
                          "actualType",
                          "scale",
                          "defaultValue",
                          "precision"]
        
        acceptable_datatypes=["esriFieldTypeBlob" ,
                              "esriFieldTypeDate",
                              "esriFieldTypeDouble" ,
                              "esriFieldTypeGeometry",
                              "esriFieldTypeGlobalID",
                              "esriFieldTypeGUID",
                              "esriFieldTypeInteger",
                              "esriFieldTypeOID",
                              "esriFieldTypeRaster",
                              "esriFieldTypeSingle",
                              "esriFieldTypeSmallInteger",
                              "esriFieldTypeString",
                              "esriFieldTypeXML"]
        

        field_prop_schema= dict(deepcopy(self.properties.fields[1]))
        for field_prop,value in field_props.items():
            assert field_prop in field_properties, "{} is not a field property. Acceptable field properties are \n\n\t {}".format(field_prop,
                                                                                                                                  " - ".join(field_properties))
            field_prop_schema[field_prop]=value
        self.lyr.manager.add_to_definition({"fields":[field_prop_schema]})
         
         
    def updateColumnData(self,df,join_field_df,join_field_attr, update_field_attr, update_field_df):
        assert isinstance(df,pd.DataFrame), 'The df positional argument must be a Dataframe'
        assert isinstance(join_field_df,str) , 'The join_field positional argument must be a string'
        assert isinstance(join_field_attr,str) , 'The join_field positional argument must be a string'
        assert isinstance(update_field_attr,str), 'The field_to_update positional argument must be a string'
        assert isinstance(update_field_df,str), 'The update_field positional argument must be a string'
        
        feats=self.lyr.query()
        if hasattr(self,'update'):
            features=self.update
            self.update=[]
        else:
            features=feats.features
            self.update=[]
        i=0
        attrs_to_df=[]
        for f in features:
            attr=f.attributes
            """add something to account for datetypes"""
            field_attr=attr[join_field_attr]
            if field_attr in df[join_field_df].tolist():
                new_data=df.loc[df[join_field_df]==field_attr,update_field_df].item()
                i+=1
                print ('Good'+ str(i))
            else:
                print('NotGood ' +str(i))
                new_data=-99999
                
            if update_field_attr.upper() =='INF' or update_field_attr.upper() =='DTH':
                update_field_attr=update_field_attr.upper()
                covid_recent_100k=int((new_data*100000)/attr['TOTPOP'])
                f.set_value(update_field_attr + "_100k",covid_recent_100k)
                
            f.set_value(update_field_attr,new_data)
            
            self.update.append(f)
            attrs_to_df.append(attr)
            
        self.update_attrs=pd.DataFrame(attrs_to_df)
        
    def pushChanges(self):
        from  math import floor
        groups=floor(len(self.update)/100)
        for group in range(groups+1):
            group=100*group
            lower=0 if group==0 else group
            print("Updating {} to {} rows".format(group,group+100))
            self.item.layers[0].edit_features(updates=self.update[group:group+100])    
