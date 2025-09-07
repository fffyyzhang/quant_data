import os
import pandas as pd
import time
from utils.config import DIR_DATA
from apis.tushare_api_wrapper import get_ths_index, get_ths_member


class ConceptComponentHandler:
    """处理概念板块和成分股数据的类"""
    
    def __init__(self):
        self.concept_dir = os.path.join(DIR_DATA, 'concept')
        self.csv_file = os.path.join(self.concept_dir, 'concept_component.csv')
        # 确保目录存在
        os.makedirs(self.concept_dir, exist_ok=True)
    
    def get_all_concepts(self):
        """获取所有板块（type=I和N）"""
        print("开始获取所有板块...")
        
        # 获取type=I的板块
        concepts_i = get_ths_index(type='I')
        print(f"获取到 type=I 板块数量: {len(concepts_i)}")
        
        time.sleep(0.2)  # API调用间隔
        
        # 获取type=N的板块  
        concepts_n = get_ths_index(type='N')
        print(f"获取到 type=N 板块数量: {len(concepts_n)}")
        
        # 合并两个类型的板块
        all_concepts = pd.concat([concepts_i, concepts_n], ignore_index=True)
        print(f"总板块数量: {len(all_concepts)}")
        
        return all_concepts
    
    def get_concept_members(self, ts_code):
        """获取指定板块的成分股"""
        result = get_ths_member(ts_code=ts_code)
        time.sleep(0.1)  # API调用间隔
        return result

    
    def process_all_data(self):
        """处理所有数据：获取板块和成分股，存储到CSV"""
        print("开始处理概念板块数据...")
        
        # 获取所有板块
        all_concepts = self.get_all_concepts()
        
        # 存储所有结果的DataFrame列表
        result_dfs = []
        
        # 遍历每个板块获取成分股
        for idx, concept in all_concepts.iterrows():
            ts_code = concept['ts_code']
            concept_name = concept['name']
            concept_type = concept['type']
            
            print(f"处理板块 {idx+1}/{len(all_concepts)}: {concept_name} ({ts_code})")
            
            # 获取成分股
            try:
                members = self.get_concept_members(ts_code)
                
                if not members.empty:
                    # 直接在成分股DataFrame上添加板块信息
                    members['concept_ts_code'] = ts_code
                    members['concept_name'] = concept_name
                    members['concept_type'] = concept_type
                    # 重命名列以保持一致性
                    members = members.rename(columns={
                        'con_code': 'stock_ts_code',
                        'con_name': 'stock_name'
                    })
                    # 选择需要的列并重排序
                    members = members[['concept_ts_code', 'concept_name', 'concept_type', 'stock_ts_code', 'stock_name']]
                else:
                    # 创建空成分股的板块记录
                    members = pd.DataFrame({
                        'concept_ts_code': [ts_code],
                        'concept_name': [concept_name],
                        'concept_type': [concept_type],
                        'stock_ts_code': [None],
                        'stock_name': [None]
                    })
                
                result_dfs.append(members)
                
            except Exception as e:
                print(f"重试后仍然获取板块 {ts_code} 成分股失败: {e}")
                # 记录失败的板块信息
                failed_df = pd.DataFrame({
                    'concept_ts_code': [ts_code],
                    'concept_name': [concept_name],
                    'concept_type': [concept_type],
                    'stock_ts_code': [None],
                    'stock_name': [None]
                })
                result_dfs.append(failed_df)
        
        # 合并所有DataFrame
        result_df = pd.concat(result_dfs, ignore_index=True)
        result_df.to_csv(self.csv_file, index=False, encoding='utf-8-sig')
        
        print(f"数据处理完成！")
        print(f"总记录数: {len(result_df)}")
        print(f"总板块数: {result_df['concept_ts_code'].nunique()}")
        print(f"总股票数: {result_df['stock_ts_code'].nunique()}")
        print(f"数据已保存到: {self.csv_file}")
        
        return result_df


if __name__ == '__main__':
    handler = ConceptComponentHandler()
    handler.process_all_data()