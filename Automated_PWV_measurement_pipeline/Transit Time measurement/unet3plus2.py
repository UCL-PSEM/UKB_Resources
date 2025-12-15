import tensorflow as tf
import layer_util 
import numpy as np

### This is the true UNet 3+ as in the paper

class unet3plus:
    def __init__(self, 
                 inputs,
                 scales = 5,
                 rank = 2, 
                 n_outputs = 3, 
                 add_dropout = False,
                 dropout_rate = 0.5,
                 base_filters = 32, 
                 kernel_size = 3,
                 pool_size = 2,
                 block_depth = 2,
                 batch_norm = True, 
                 CGM = False, 
                 supervision= True):
        
        
        self.inputs = inputs
        self.scales = scales
        self.rank = rank
        self.n_outputs = n_outputs
        self.base_filters = base_filters
        self.kernel_size = kernel_size
        self.add_dropout = add_dropout
        self.dropout_rate = dropout_rate  
        self.block_depth = block_depth      
        self.batch_norm = batch_norm

        # Assign pool size
        if isinstance(pool_size,tuple):
            self.pool_size = pool_size
        else:
            self.pool_size = tuple([pool_size for _ in range(rank)])
        

        # Check if Unet can be constructed with given input dimensions
        #in_shape = inputs.shape[1:-1]
        #print(in_shape)
        #print(scales-1)
        #if (in_shape%(np.array(self.pool_size)**(scales-1))).any() > 0:
        #    raise ValueError("All input dimensions must be divisible by pool_size^(scales)")
        
        self.CGM = CGM
        self.supervision = supervision
        self.conv_config = dict(kernel_size = 3,
                           padding = 'same',
                           kernel_initializer = 'he_normal')
    
    def aggregate(self, scale_list, scale): #decoders: aggregate all inputs into one block
        X = tf.keras.layers.Concatenate(name = f'D{scale}_input', axis = -1)(scale_list)
        X = self.conv_block(X, self.base_filters * self.scales, num_stacks = self.block_depth, layer_type = 'Decoder', scale=scale)
        return X

    def deep_sup(self, inputs, scale): #deep supervision-- supervision at all layers --I choose false for this 
        conv = layer_util.get_nd_layer('Conv', self.rank)
        upsamp = layer_util.get_nd_layer('UpSampling', self.rank)
        size = tuple(np.array(self.pool_size)** (abs(scale-1)))
        if self.rank == 2:
            upsamp_config = dict(size=size, interpolation='bilinear')
        else:
            upsamp_config = dict(size=size)  
        X = inputs  
        X = conv(self.n_outputs, activation = None, **self.conv_config, name = f'deepsup_conv_{scale}')(X)
        if scale != 1:
            X = upsamp(**upsamp_config, name = f'deepsup_upsamp_{scale}')(X)
        X = tf.keras.layers.Activation(activation = 'sigmoid' if self.n_outputs == 1 else 'softmax', name = f'deepsup_activation_{scale}')(X)
        return X
        
        
        
    def full_scale(self, inputs, to_layer, from_layer): #between layers: define all the skip connections and down/up sampling
        conv = layer_util.get_nd_layer('Conv', self.rank)
        layer_diff = from_layer - to_layer  
        size = tuple(np.array(self.pool_size)** (abs(layer_diff)))
        maxpool = layer_util.get_nd_layer('MaxPool', self.rank)
        upsamp = layer_util.get_nd_layer('UpSampling', self.rank)
        if self.rank == 2:
            upsamp_config = dict(size=size, interpolation='bilinear')
        else:
            upsamp_config = dict(size=size)
        
        X = inputs        
        if to_layer < from_layer:
            X = upsamp(**upsamp_config, name = f'Skip_Upsample_{from_layer}_{to_layer}')(X)
        elif to_layer > from_layer:
            X = maxpool(pool_size = size, name = f'Skip_Maxpool_{from_layer}_{to_layer}')(X)
        X = conv(self.base_filters,**self.conv_config, name = f'Skip_Conv_{from_layer}_{to_layer}')(X)
        return X
        
    def conv_block(self, inputs, filters, num_stacks,layer_type, scale): #within each layer: Convolution block at each layer, activation as LeakyReLU
        conv = layer_util.get_nd_layer('Conv', self.rank)
        X = inputs
        for i in range(num_stacks):
            X = conv(filters, **self.conv_config, name = f'{layer_type}{scale}_Conv_{i+1}')(X)
            if self.batch_norm:
                X = tf.keras.layers.BatchNormalization(axis=-1, name = f'{layer_type}{scale}_BN_{i+1}')(X)
            X = tf.keras.layers.LeakyReLU(name = f'{layer_type}{scale}_Activation_{i+1}')(X)
        return X
    
    
    def encode(self, inputs, scale, num_stacks):
        maxpool = layer_util.get_nd_layer('MaxPool', self.rank)
        scale -= 1 # python index
        filters = self.base_filters * 2 ** scale
        
        X = inputs
        if scale != 0:
            X = maxpool(pool_size=self.pool_size, name = f'encoding_{scale}_maxpool')(X)
        X = self.conv_block(X, filters, num_stacks, layer_type = 'Encoder', scale = scale+1)
        if scale == (self.scales-1) and self.add_dropout:
            X = tf.keras.layers.Dropout(rate = self.dropout_rate, name = f'Encoder{scale+1}_dropout')(X)
        return X
        
    def outputs(self): #make everything together into a decoder of the network
        XE  = [self.inputs]
        for i in range(self.scales):
            XE.append(self.encode(XE[i], scale = i+1, num_stacks = self.block_depth))

        XD = [XE[-1]]
        for decoder_level in range(self.scales-1,0,-1):
            skip_contributions = []
            # Append skips from encoder
            for encoder_level in range(1,decoder_level+1):
                skip_contributions.append(self.full_scale(XE[encoder_level], decoder_level, encoder_level))
            # Append skips from decoder
            for i in range(len(XD)-1,-1,-1):
                skip_contributions.append(self.full_scale(XD[i], decoder_level, (self.scales-i)))
            XD.append(self.aggregate(skip_contributions,decoder_level))


        if self.supervision == True:
            XD = [self.deep_sup(xd, self.scales-i) for i,xd in enumerate(XD)]
            return XD
        else:
            XD[-1] = self.deep_sup(XD[-1],1)
            return XD[-1]

