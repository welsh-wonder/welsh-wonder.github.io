#!/usr/bin/env python
# coding: utf-8

# In[625]:


import pandas as pd
import datacompy


# In[626]:


import pyodbc
conn = pyodbc.connect('DSN=SysproCompanyB;Trusted_Connection=yes;')
cursor = conn.cursor()


# ### Stocking Points / Bom Structure

# In[580]:


syspro_bom_structure = '''

/* Standard Routing */

WITH GetBomHierarchy AS
(
    SELECT
                  inv.StockCode
                , bs.ParentPart
                , bs.Component
                , 1 AS LevelNumber
    FROM
                SysproCompanyB.dbo.BomStructure bs
    INNER JOIN
                SysproCompanyB.dbo.InvMaster inv
    ON
                bs.ParentPart = inv.StockCode
    WHERE
                bs.Route = 0
    AND
                LEFT(inv.StockCode, 1) <> 5

    UNION ALL

    SELECT
                  gbh.StockCode
                , bs.ParentPart
                , bs.Component
                , gbh.LevelNumber + 1 AS LevelNumber
    FROM
                SysproCompanyB.dbo.BomStructure bs
    INNER JOIN
                GetBomHierarchy gbh
    ON
                bs.ParentPart = gbh.Component
)
    SELECT DISTINCT
                      StockCode AS ParentPart
                    , ParentPart AS Component
                    --, LevelNumber
    FROM
                    GetBomHierarchy lf
'''


# In[581]:


syspro_p_bom = pd.read_sql_query(syspro_bom_structure, conn)
syspro_p_bom = syspro_p_bom.sort_values(by='ParentPart')


# In[582]:


syspro_p_bom


# In[583]:


quintiq_bom_structure = '''
WITH GetQuintiqBomRoutes AS
(
    SELECT
                  prsm.InputProductId
                , ip.ProductKey AS InputProductKey
                , prsm.ProductId
                , p.ProductKey
                , prsm.RoutingId
                , prsm.StockingPointId AS InputStockingPointId
                , p.StockingPointId AS ProductStockPointId
    FROM    
                QuintiqIDB.dbo.QRG_ProductionRoutingStepInputMaterial prsm
    LEFT JOIN 
                QuintiqIDB.dbo.QRG_ProductionRouting pr
    ON      
                prsm.RoutingId = pr.RoutingId
    LEFT JOIN  
                QuintiqIDB.dbo.QRG_Product p
    ON
                prsm.ProductId = p.ProductId
    LEFT JOIN
                QuintiqIDB.dbo.QRG_Product ip
    ON
                prsm.InputProductId = ip.ProductId
    WHERE   
                prsm.StockingPointId <> 'CON'
    AND 
                pr.Preferred = '1'
)
, GetBomRouteHierarchy AS
(
    SELECT
                  rt.ProductKey AS FinalProductKey
                , rt.InputProductId
                , rt.InputProductKey
                --, rt.ProductId
                , rt.ProductKey
                --, rt.RoutingId
                --, rt.InputStockingPointId
                --, rt.ProductStockPointId
                , 1 AS LevelNumber
    FROM
                GetQuintiqBomRoutes rt
    WHERE
                ProductStockPointId = 'FG'

    UNION ALL

    SELECT
                  rt.FinalProductKey
                , lv.InputProductId
                , lv.InputProductKey
                --, lv.ProductId
                , lv.ProductKey
                --, lv.RoutingId
                --, lv.InputStockingPointId
                --, lv.ProductStockPointId
                , rt.LevelNumber + 1 AS LevelNumber
    FROM
                GetQuintiqBomRoutes lv
    INNER JOIN
                GetBomRouteHierarchy rt
    ON
                rt.InputProductId = lv.ProductId
            
)
SELECT DISTINCT
              --FinalProductKey
            --, InputProductKey
            --, ProductKey
            --, 
            SUBSTRING(FinalProductKey, 4, 8) AS ParentPart
            , ISNULL(invp.StockCode, SUBSTRING(FinalProductKey, 4, 8)) AS Component
            --, LevelNumber
FROM 
            GetBomRouteHierarchy prt
LEFT JOIN
            SysproCompanyB.dbo.[InvMaster+] invp
ON
            prt.ProductKey = invp.QuintiqProductKey;
'''


# In[584]:


quintiq_p_bom = pd.read_sql_query(quintiq_bom_structure, conn)
quintiq_p_bom = quintiq_p_bom.sort_values(by='ParentPart')


# In[585]:


quintiq_p_bom = quintiq_p_bom[['ParentPart', 'Component']]
quintiq_p_bom


# In[586]:


compare = datacompy.Compare(
    df1 = syspro_p_bom,
    df2 = quintiq_p_bom,
    join_columns=['ParentPart'],  #You can also specify a list of columns
    abs_tol=0, #Optional, defaults to 0
    rel_tol=0, #Optional, defaults to 0
    df1_name ='syspro', #Optional, defaults to 'df1'
    df2_name ='quintiq' #Optional, defaults to 'df2'
    )
compare.matches(ignore_extra_columns=False)
# False

# This method prints out a human-readable report summarizing and sampling differences
#print(compare.report(sample_count=12741))

#csvFileToWrite=r'c:\Users\mwelch\data_comparison.csv'

#with open(csvFileToWrite,mode='r+',encoding='utf-8') as report_file:
        #report_file.write(compare.report(sample_count=14544))
syspro_not_quintiq = compare.df1_unq_rows
quintiq_not_syspro = compare.df2_unq_rows
diff = compare.sample_mismatch('component', sample_count=12741, for_display=True)


# #### Components/Stocking Points in Syspro and not in Quintiq

# In[587]:


syspro_not_quintiq


# #### Components/Stocking Points in Quintiq and not in Syspro

# In[588]:


quintiq_not_syspro


# #### Matched Parent with Different Component

# In[589]:


diff


# ## Production Routing Steps & Bom Operations

# ### SysproCompanyB Preferred Routes Query (Code)

# In[870]:


# syspro companyb db query
syspro_sql_query = '''

/* Standard Routing */

WITH GetBomHierarchy AS
(
    SELECT
                  inv.StockCode
                , bs.ParentPart
                , bs.Component
                , 1 AS LevelNumber
    FROM
                SysproCompanyB.dbo.BomStructure bs
    INNER JOIN
                SysproCompanyB.dbo.InvMaster inv
    ON
                bs.ParentPart = inv.StockCode
    WHERE
                bs.Route = 0
    AND
                LEFT(inv.StockCode, 1) <> 5

    UNION ALL

    SELECT
                  gbh.StockCode
                , bs.ParentPart
                , bs.Component
                , gbh.LevelNumber + 1 AS LevelNumber
    FROM
                SysproCompanyB.dbo.BomStructure bs
    INNER JOIN
                GetBomHierarchy gbh
    ON
                bs.ParentPart = gbh.Component
)
, GroupInterimProducts AS
(
    SELECT DISTINCT
                      StockCode
                    , LevelNumber
                    , ParentPart
    FROM
                    GetBomHierarchy lf
)
SELECT
              gbh.StockCode
            --, gbh.LevelNumber
            , gbh.ParentPart AS InterimStockCode
            --, CAST(bop.Operation AS INT) Operation
            , bop.WorkCentre
            , ISNULL(NULLIF(bop.ToolSet, ''), 'None') AS ToolSet
            , bop.IQuantity
            , CASE 
                    WHEN bwc.TimeUom = 'hrs' THEN (bop.ITimeTaken * 60)
                    ELSE bop.ITimeTaken
              END 
                    ProductionTimeMins
            --, bwc.TimeUom
FROM 
            GroupInterimProducts gbh
LEFT JOIN
            SysproCompanyB.dbo.BomOperations bop
ON
            gbh.ParentPart = bop.StockCode
LEFT JOIN
            SysproCompanyB.dbo.BomWorkCentre bwc
ON
            bop.WorkCentre = bwc.WorkCentre
LEFT JOIN
            SysproCompanyB.dbo.zWorkCentreCost wcc
ON
            bwc.WorkCentre = wcc.WorkCentre
LEFT JOIN
            SysproCompanyB.dbo.[InvMaster+] invp
ON
            gbh.StockCode = invp.StockCode
LEFT JOIN
            SysproCompanyB.dbo.InvMaster inv
ON
            gbh.StockCode = inv.StockCode
WHERE
            bop.WorkCentre NOT IN ('PACK')
ORDER BY
            gbh.StockCode ASC, gbh.LevelNumber DESC, bop.Operation ASC
'''


# ### Extract & Tag Data Frame with Data Source

# In[871]:


# extract syspro routes from sysprocompanyb
syspro_p_routes = pd.read_sql_query(syspro_sql_query, conn)
syspro_p_routes['source'] = 'SysproCompanyB'
syspro_p_routes


# ### Quintiq IDB Preferred Routes Query (Code)

# In[872]:


# quintiq integration db query
quintiq_sql_query = '''

/* Standard Routing */

WITH GetQuintiqBomRoutes AS
(
    SELECT
                  prsm.InputProductId
                , prsm.ProductId
                , prsm.RoutingId
                , prsm.StockingPointId AS InputStockingPointId
                , p.StockingPointId AS ProductStockPointId
    FROM    
                QuintiqIDB.dbo.QRG_ProductionRoutingStepInputMaterial prsm
    LEFT JOIN 
                QuintiqIDB.dbo.QRG_ProductionRouting pr
    ON      
                prsm.RoutingId = pr.RoutingId
    LEFT JOIN  
                QuintiqIDB.dbo.QRG_Product p
    ON
                prsm.ProductId = p.ProductId
    LEFT JOIN
                QuintiqIDB.dbo.QRG_Product ip
    ON
                prsm.InputProductId = ip.ProductId
    WHERE   
                prsm.StockingPointId <> 'CON'
    AND 
                pr.Preferred = '1'
)
, GetBomRouteHierarchy AS
(
    SELECT
                  rt.ProductId AS FinalProductId
                , rt.InputProductId
                , rt.ProductId
                , rt.RoutingId
                , rt.InputStockingPointId
                , rt.ProductStockPointId
                , 1 AS LevelNumber
    FROM
                GetQuintiqBomRoutes rt
    WHERE
                ProductStockPointId = 'FG'

    UNION ALL

    SELECT
                  rt.FinalProductId
                , lv.InputProductId
                , lv.ProductId
                , lv.RoutingId
                , lv.InputStockingPointId
                , lv.ProductStockPointId
                , rt.LevelNumber + 1 AS LevelNumber
    FROM
                GetQuintiqBomRoutes lv
    INNER JOIN
                GetBomRouteHierarchy rt
    ON
                rt.InputProductId = lv.ProductId
            
)
SELECT  
              fp.Stockcode
            , ISNULL(NULLIF(invp.StockCode, ''), fp.Stockcode) AS InterimStockCode
            , prs.ResourceGroup AS WorkCentre
            , ISNULL(NULLIF(prs.PassCode, ''), 'None') AS ToolSet
            , ISNULL((pp.PieceWeight / 1000), (fp.PieceWeight / 1000)) AS IQuantity
            , ISNULL(prs.ProductionTime,0) * 24 * 60 ProductionTimeMins
FROM    
            QuintiqIDB.dbo.QRG_ProductionRouting pr
INNER JOIN 
            QuintiqIDB.dbo.QRG_ProductionRoutingStep prs
ON      
            pr.RoutingId = prs.RoutingId
INNER JOIN 
            QuintiqIDB.dbo.QRG_ProductionRoutingStepInputMaterial prsm
ON      
            pr.RoutingId = prsm.RoutingId
AND 
            prsm.StockingPointId <> 'CON'
INNER JOIN 
            GetBomRouteHierarchy ot
ON      
            prsm.RoutingId = ot.RoutingId
INNER JOIN 
            QuintiqIDB.dbo.QRG_Product p
ON      
            ot.InputProductId = p.ProductId
LEFT JOIN
            QuintiqIDB.dbo.QRG_Product fp
ON
            ot.FinalProductId = fp.ProductId
LEFT JOIN
            QuintiqIDB.dbo.QRG_Product pp
ON
            ot.ProductId = pp.ProductId
LEFT JOIN
            SysproCompanyB.dbo.[InvMaster+] invp
ON
            pp.ProductKey = invp.QuintiqProductKey
WHERE
            prs.ResourceGroup NOT IN ('Transport1', 'Test1')
ORDER BY 
              fp.Stockcode
            , ot.LevelNumber DESC
            , prs.SequenceNr ASC
'''


# ### Extract & Tag Data Frame with Data Source

# In[873]:


# extract quintiq routes from quintiq integration db
quintiq_p_routes = pd.read_sql_query(quintiq_sql_query, conn)
quintiq_p_routes['source'] = 'QuintiqIDB'
quintiq_p_routes


# ### DataComPY Library

# In[874]:


# data compy
compare = datacompy.Compare(
    df1 = syspro_p_routes,
    df2 = quintiq_p_routes,
    join_columns=['StockCode', 'InterimStockCode', 'WorkCentre', 'ToolSet'],  #You can also specify a list of columns
    abs_tol=0, #Optional, defaults to 0
    rel_tol=0, #Optional, defaults to 0
    df1_name ='syspro', #Optional, defaults to 'df1'
    df2_name ='quintiq' #Optional, defaults to 'df2'
    )
compare.matches(ignore_extra_columns=False)
# False

# This method prints out a human-readable report summarizing and sampling differences
print(compare.report(sample_count=12741))

#csvFileToWrite=r'c:\Users\mwelch\data_comparison.csv'

#with open(csvFileToWrite,mode='r+',encoding='utf-8') as report_file:
        #report_file.write(compare.report(sample_count=14544))

time_diff = compare.sample_mismatch('productiontimemins', sample_count=12764, for_display=True)
quantity_diff = compare.sample_mismatch('iquantity', sample_count=12764, for_display=True)

syspro_not_quintiq = compare.df1_unq_rows
quintiq_not_syspro = compare.df2_unq_rows


# ### Tag Data Frames from 'sample_mismatch' Method

# In[875]:


#calculate time variance for matching records across data sources
time_diff['variance (mins)'] = time_diff['productiontimemins (syspro)'] - time_diff['productiontimemins (quintiq)']
time_diff


# In[876]:


# calculate quantity variance for matching records across data sources
quantity_diff['variance'] = quantity_diff['iquantity (syspro)'] - quantity_diff['iquantity (quintiq)']
quantity_diff


# ### Tag Data Frames from 'df_unq_rows' Method

# In[877]:


# syspro_not_quintiq
# add boolean column set to true
syspro_not_quintiq['not_in_quintiq'] = 1
syspro_not_quintiq


# In[878]:


# quintiq_not_syspro
# add boolean column set to true
quintiq_not_syspro['not_in_syspro'] = 1
quintiq_not_syspro


# ### Syspro Company B

# #### Data Leakage

# In[879]:


# test existence / leakage
syspro_p_routes = syspro_p_routes.merge(syspro_not_quintiq[['stockcode', 'interimstockcode', 'workcentre', 'toolset', 
                                                            'not_in_quintiq']], how='left', left_on=['stockcode', 
                                                            'interimstockcode', 'workcentre', 'toolset'], right_on=['stockcode', 
                                                            'interimstockcode', 'workcentre', 'toolset'])
syspro_p_routes['not_in_quintiq'] = syspro_p_routes['not_in_quintiq'].fillna(value=0)


# #### Time Variance

# In[880]:


# production minutes
syspro_p_routes = syspro_p_routes.merge(time_diff[['stockcode', 'interimstockcode', 'workcentre', 'toolset', 
                                                   'productiontimemins (quintiq)']], how='left', left_on=['stockcode', 
                                                   'interimstockcode', 'workcentre', 'toolset'], right_on=['stockcode', 
                                                   'interimstockcode', 'workcentre', 'toolset'])


# In[881]:


# calculate time variance
syspro_p_routes['time_variance'] = syspro_p_routes['productiontimemins'] - syspro_p_routes['productiontimemins (quintiq)']
syspro_p_routes


# #### Quantity/Weight Variance

# In[882]:


# # quantity/weight variance
# syspro_p_routes = syspro_p_routes.merge(quantity_diff[['stockcode', 'interimstockcode', 'workcentre', 'toolset', 
#                                                    'iquantity (quintiq)']], how='left', left_on=['stockcode', 
#                                                    'interimstockcode', 'workcentre', 'toolset'], right_on=['stockcode', 
#                                                    'interimstockcode', 'workcentre', 'toolset'])


# In[883]:


# # calculate time variance
# syspro_p_routes['qty_variance'] = syspro_p_routes['iquantity'] - syspro_p_routes['iquantity (quintiq)']
# syspro_p_routes


# ### Quintiq IDB Preferred Routes

# #### Data Leakage

# In[884]:


# test existence / leakage
quintiq_p_routes = quintiq_p_routes.merge(quintiq_not_syspro[['stockcode', 'interimstockcode', 'workcentre', 'toolset', 
                                                              'not_in_syspro']], how='left', left_on=['stockcode', 
                                                              'interimstockcode', 'workcentre', 'toolset'], right_on=['stockcode', 
                                                              'interimstockcode', 'workcentre', 'toolset'])
quintiq_p_routes['not_in_syspro'] = quintiq_p_routes['not_in_syspro'].fillna(value=0)
quintiq_p_routes


# #### Time Variance

# In[885]:


# production minutes
quintiq_p_routes = quintiq_p_routes.merge(time_diff[['stockcode', 'interimstockcode', 'workcentre', 'toolset', 
                                                              'productiontimemins (syspro)']], how='left', left_on=['stockcode', 
                                                              'interimstockcode', 'workcentre', 'toolset'], right_on=['stockcode', 
                                                              'interimstockcode', 'workcentre', 'toolset'])


# In[886]:


# calculate time variance
quintiq_p_routes['time_variance'] = quintiq_p_routes['productiontimemins'] - quintiq_p_routes['productiontimemins (syspro)']
quintiq_p_routes


# #### Quantity/Weight Variance

# In[887]:


# # qty/tonnes variance
# quintiq_p_routes = quintiq_p_routes.merge(quantity_diff[['stockcode', 'interimstockcode', 'workcentre', 'toolset', 
#                                                               'iquantity (syspro)']], how='left', left_on=['stockcode', 
#                                                               'interimstockcode', 'workcentre', 'toolset'], right_on=['stockcode', 
#                                                               'interimstockcode', 'workcentre', 'toolset'])


# In[888]:


# # calculate qty variance
# quintiq_p_routes['qty_variance'] = quintiq_p_routes['iquantity'] - quintiq_p_routes['iquantity (syspro)']
# quintiq_p_routes


# ### Combine Final Data Set

# In[902]:


# combine, append, union the two data frames
combined_rts = pd.concat([syspro_p_routes, quintiq_p_routes])


# In[903]:


combined_rts = combined_rts[['stockcode', 'interimstockcode', 'workcentre', 'toolset', 'source', 'not_in_quintiq', 'not_in_syspro', 
              #'iquantity', 'iquantity (syspro)', 'iquantity (quintiq)', 'qty_variance', 
                             'productiontimemins', 
              'productiontimemins (syspro)', 'productiontimemins (quintiq)', 'time_variance']]

combined_rts['not_in_syspro'] = combined_rts['not_in_syspro'].fillna(value='N/A')
combined_rts['not_in_quintiq'] = combined_rts['not_in_quintiq'].fillna(value='N/A')
combined_rts['productiontimemins (syspro)'] = combined_rts['productiontimemins (syspro)'].fillna(value='N/A')
combined_rts['productiontimemins (quintiq)'] = combined_rts['productiontimemins (quintiq)'].fillna(value='N/A')
combined_rts['time_variance'] = combined_rts['time_variance'].fillna(value=0)

combined_rts


# In[900]:


combined_rts.to_csv('data_comparison.csv')

