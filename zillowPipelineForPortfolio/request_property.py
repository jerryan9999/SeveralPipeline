import requests
import json
import psycopg2
import config
import datetime
import multiprocessing as mp

conn = psycopg2.connect(config.SQL_STR)
cursor = conn.cursor()

master_ref_details = {
    "source_id":{"level":0,"upper":None,"key":"source_id"},
    "latitude":{"level":2,"upper":"location.coordinates","key":1},
    "longitude":{"level":2,"upper":"location.coordinates","key":0},
    "squarefeet":{"level":0,"upper":None,"key":"size"},

    "bathrooms":{"level":0,"upper":None,"key":"baths"},
    "bedrooms":{"level":0,"upper":None,"key":"beds"},
    "yearbuilt":{"level":0,"upper":None,"key":"year_built"},
    "propertytype":{"level":0,"upper":None,"key":"room_type"},
    #"lotsize":{"level":1,"upper":"physical","key":"lotSize"},
    #"ispool":{"level":1,"upper":"physical","key":"isPool"},
    "address1":{"level":0,"upper":None,"key":"addr"},
    "zip":{"level":0,"upper":None,"key":"zipcode"},
    "city":{"level":0,"upper":None,"key":"city"},
    #"county":{"level":1,"upper":"address","key":"county"},
    "cbsacode":{"level":1,"upper":"area","key":"id"},
    "state":{"level":0,"upper":None,"key":"state"},

    "listprice":{"level":0,"upper":None,"key":"house_price_dollar"},
    "monthlyrent":{"level":0,"upper":None,"key":"rent"},
    #"yearlyinsurancecost":{"level":1,"upper":"financial","key":"yearlyInsuranceCost"},
    #"yearlypropertytaxes":{"level":1,"upper":"financial","key":"yearlyPropertyTaxes"},
    "appreciation":{"level":0,"upper":None,"key":"increase_ratio"},
    #"caprate":{"level":1,"upper":"computed","key":"capRate"},
    "neighborscore":{"level":1,"upper":"neighborhood","key":"score"},
    "score":{"level":0,"upper":None,"key":"score"},

    "status":{"level":0,"upper":None,"key":"status"},
    "imgurl":{"level":1,"upper":"pict_urls","key":0},
    "neighbor_regionid":{"level":1,"upper":"neighborhood","key":"id"},
}

master_ref_invest = {
    "caprate":{"level":1,"upper":"initial_data","key":"cap_rate"},
    "irr":{"level":1,"upper":"initial_data","key":"irr"},
    "yearlyinsurancecost":{"level":1,"upper":"total_expenses","key":"insurance_cost"},
    "yearlypropertytaxes":{"level":1,"upper":"total_expenses","key":"property_tax"}
}


def strip_key_info(ref,house):
  it = {}
  for k,v in ref.items():
    try:
      if v['level']==0:
        it[k] = house[v['key']] 
      elif v['level']==1:
        it[k] = house[v['upper']][v['key']]
      elif v['level']==2:
        ks = v['upper'].split('.')
        if len(ks)==2: 
          it[k] = house[ks[0]][ks[1]][v['key']]
    except Exception as e:
      print e

  return it

def send_request(url,zpid):
    # homedetails
    # POST http://test.fanglimei.cn/api/homedetails

    try:
        response = requests.post(
            url=url,
            headers={
                "content-type": "application/json",
                "Authorization": "eyJhbGciOiJIUzI1NiIsImV4cCI6NDY2OTM0OTg1OSwiaWF0IjoxNTE1NzQ5ODU5fQ.eyJyb2xlIjowLCJpZCI6OCwibmFtZSI6ImFwb2NhbHlwc2UifQ.N3FxjlGuWU5RtoJE9SYQeUptjQHyNoM9qRYdQNJLXPg",
            },
            data=json.dumps({
                "home_id": "{}_zillow".format(zpid),
                "down_payment":0.4
            })
        )
        try:
          item = json.loads(response.content)
          return item
        except:
          pass
    except requests.exceptions.RequestException:
        print('HTTP Request failed')

def update2_db(item):
  sql = """INSERT INTO property(
              source,
              source_id,
              latitude,
              longitude,
              squarefeet,

              bathrooms,
              bedrooms,
              yearbuilt,
              propertytype,


              address1,
              zip,
              city,

              cbsacode,
              state,

              listprice,
              monthlyrent,
              yearlyinsurancecost,
              yearlypropertytaxes,
              appreciation,
              status,

              created_at,
              updated_at,
              neighborscore,
              imgurl,
              caprate,
              irr,
              neighbor_regionid
      )
      VALUES (
              %(source)s,
              %(source_id)s,
              %(latitude)s,
              %(longitude)s,
              %(squarefeet)s,

              %(bathrooms)s,
              %(bedrooms)s,
              %(yearbuilt)s,
              %(propertytype)s,


              %(address1)s,
              %(zip)s,
              %(city)s,

              %(cbsacode)s,
              %(state)s,

              %(listprice)s,
              %(monthlyrent)s,
              %(yearlyinsurancecost)s,
              %(yearlypropertytaxes)s,
              %(appreciation)s,
              %(status)s,

              %(created_at)s,
              %(updated_at)s,
              %(neighborscore)s,
              %(imgurl)s,
              %(caprate)s,
              %(irr)s,
              %(neighbor_regionid)s
      )
      ON CONFLICT ON CONSTRAINT unique_property_constraint
      DO UPDATE SET
        status=%(status)s,
        updated_at=%(updated_at)s,
        listprice=%(listprice)s,
        caprate=%(caprate)s,
        irr = %(irr)s,
        neighbor_regionid=%(neighbor_regionid)s
    """
  cursor.execute(sql,item)
  conn.commit()


if __name__ == '__main__':

    api_homedetails = "http://test.fanglimei.cn/api/homedetails"
    api_investment_details = "http://test.fanglimei.cn/api/homeinvestment"

    zpids = []
    with open('temp.csv') as f:
      for line in f:
        zpids.append(line.strip('\n'))

    for index,zpid in enumerate(zpids):
      #if index<2826:
      #  continue

      item_part1 = send_request(url=api_homedetails,zpid=zpid)
      item_part2 = send_request(url=api_investment_details,zpid=zpid)

      if not item_part1:
        continue

      item_p1 = strip_key_info(ref=master_ref_details,house=item_part1['result'][0])
      item_p2 = strip_key_info(ref=master_ref_invest,house=item_part2)
      
      item = {}
      item.update(item_p1)
      item.update(item_p2)

      #add more field
      item['source'] = 'zillow'
      if item['status']==2:
        item['status'] = 'ForSale'
      elif item['status']==4:
        item['status'] = 'Sold'

      now = datetime.datetime.now()
      item['created_at'] = now
      item['updated_at'] = now
      item['irr']=item['irr']/100 if item.get('irr') else None
      item['caprate']=item['caprate']/100 if item.get('caprate') else None
      if not item.get('imgurl'):
        continue
      if not item.get('neighborscore'):item['neighborscore'] = None
      if not item.get('yearlyinsurancecost'):item['yearlyinsurancecost'] = None
      if not item.get('yearlypropertytaxes'):item['yearlypropertytaxes'] = None
      print(index)
      update2_db(item)
