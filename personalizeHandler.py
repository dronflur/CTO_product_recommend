import pandas as pd
import time

def personalize(alsModel, SavePath):
    col_user = []
    col_recommend = []
    sum = 0
    iteration = 0
    for key, value in alsModel.users.items():
        user_id = value
        user_idNew = key
        start = time.time()
        dfRec = alsModel.model.recommendProducts(int(user_idNew), 10)
        stop = time.time()
        sum = sum + (stop - start)
        lst_rec = [alsModel.products[i[1]] for i in dfRec]
        print(str(iteration) + ": user_idNew " + str(user_idNew) + " list: " + str(lst_rec))

        col_user.append(user_id)
        col_recommend.append(lst_rec)
        print("---- appended ----")
        iteration = iteration + 1
    '''
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
    '''
    print("avg recommendProducts: " + str(sum/len(alsModel.users)))
    dfProductRec = pd.DataFrame()
    dfProductRec['UserId'] = col_user
    dfProductRec['Recommend'] = col_recommend
    return dfProductRec