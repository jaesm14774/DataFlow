import numpy as np
import cv2
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Conv2D, MaxPooling2D, BatchNormalization, Flatten, Dropout, Dense

def create_cnn_model(width, height, allowedChars, num_digit):
    print('Creating CNN model with TensorFlow 2.16.2...')
    tensor_in = Input((height, width, 3), name='input_layer')

    tensor_out = tensor_in
    tensor_out = Conv2D(filters=32, kernel_size=(3, 3), padding='same', activation='relu')(tensor_out)
    tensor_out = Conv2D(filters=32, kernel_size=(3, 3), padding='same', activation='relu')(tensor_out)
    tensor_out = MaxPooling2D(pool_size=(2, 2))(tensor_out)
    tensor_out = Conv2D(filters=64, kernel_size=(3, 3), padding='same', activation='relu')(tensor_out)
    tensor_out = Conv2D(filters=64, kernel_size=(3, 3), padding='same', activation='relu')(tensor_out)
    tensor_out = MaxPooling2D(pool_size=(2, 2))(tensor_out)
    tensor_out = Conv2D(filters=128, kernel_size=(3, 3), padding='same', activation='relu')(tensor_out)
    tensor_out = Conv2D(filters=128, kernel_size=(3, 3), padding='same', activation='relu')(tensor_out)
    tensor_out = BatchNormalization()(tensor_out)
    tensor_out = MaxPooling2D(pool_size=(2, 2))(tensor_out)
    tensor_out = Conv2D(filters=256, kernel_size=(3, 3), padding='same', activation='relu')(tensor_out)
    tensor_out = Conv2D(filters=256, kernel_size=(3, 3), padding='same', activation='relu')(tensor_out)
    tensor_out = MaxPooling2D(pool_size=(2, 2))(tensor_out)
    tensor_out = Conv2D(filters=512, kernel_size=(3, 3), padding='same', activation='relu')(tensor_out)
    tensor_out = BatchNormalization()(tensor_out)
    tensor_out = MaxPooling2D(pool_size=(2, 2))(tensor_out)

    tensor_out = Flatten()(tensor_out)
    tensor_out = Dropout(0.5)(tensor_out)

    outputs = []
    for i in range(1, num_digit + 1):
        digit = Dense(len(allowedChars), name=f'digit{i}', activation='softmax')(tensor_out)
        outputs.append(digit)
    
    model = Model(inputs=tensor_in, outputs=outputs)
    
    # 为每个输出指定相同的损失函数和度量标准
    losses = {f'digit{i}': 'categorical_crossentropy' for i in range(1, num_digit + 1)}
    metrics = {f'digit{i}': 'accuracy' for i in range(1, num_digit + 1)}
    
    model.compile(
        loss=losses, 
        optimizer='adamax', 
        metrics=metrics
    )
    model.summary()
    
    return model

def preprocessing(img_path, save_path=None):
    """预处理验证码图像"""
    img = cv2.imread(img_path)
    
    # 转换为灰度图
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    
    # 二值化处理
    _, binary = cv2.threshold(gray, 127, 255, cv2.THRESH_BINARY_INV)
    
    # 去噪
    kernel = np.ones((2, 2), np.uint8)
    opening = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel)
    
    # 转回彩色图像以符合模型输入要求
    processed = cv2.cvtColor(opening, cv2.COLOR_GRAY2BGR)
    
    if save_path:
        cv2.imwrite(save_path, processed)
    
    return processed

def one_hot_encoding(text, allowedChars):
    """将文本转换为one-hot编码"""
    result = []
    for char in text:
        vector = [0] * len(allowedChars)
        if char in allowedChars:
            vector[allowedChars.index(char)] = 1
        result.append(vector)
    return result

def one_hot_decoding(predictions, allowedChars):
    """将模型预测结果转换回文本"""
    result = ""
    for pred in predictions:
        index = np.argmax(pred)
        if index < len(allowedChars):
            result += allowedChars[index]
    return result

def show_train_history(history, train_metric, val_metric):
    """显示训练历史"""
    import matplotlib.pyplot as plt
    plt.figure(figsize=(12, 6))
    plt.plot(history.history[train_metric])
    plt.plot(history.history[val_metric])
    plt.title(f'Train History: {train_metric}')
    plt.ylabel('Accuracy')
    plt.xlabel('Epoch')
    plt.legend(['train', 'validation'], loc='upper left')
    plt.savefig('train_history.png')
    plt.show()