import pandas as pd

def personalize(alsModel, SavePath):
    col_user = []
    col_recommend = []

    for idx, user in alsModel.users.iterrows():
    
        user_id = user['UserId']
        user_idNew = user['UserIdNew']
        lst_rec = []

        # Get Recommend
        dfRec = alsModel.model.recommendProducts(int(user_idNew), 10)
        for row in dfRec:
            pdProduct_Fre = alsModel.products.loc[alsModel.products['PidNew'] == row[1]]
            product = str(list(pdProduct_Fre.Pid)[0])
            lst_rec.append(product)

        col_user.append(user_id)
        col_recommend.append(lst_rec)
        
    dfProductRec = pd.DataFrame()
    dfProductRec['UserId'] = col_user
    dfProductRec['Recommend'] = col_recommend
    return dfProductRec