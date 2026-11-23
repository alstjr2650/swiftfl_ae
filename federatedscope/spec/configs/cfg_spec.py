from federatedscope.core.configs.config import CN
from federatedscope.register import register_config

def extend_spec_cfg(cfg):
    cfg.traffic = CN()
    cfg.traffic.enabled = False
    cfg.traffic.policy = 'fifo'
    cfg.traffic.profile = 'H'
    cfg.traffic.shared_mbps = 30
    cfg.traffic.chunk_bytes = 16384
    cfg.traffic.drop_start_s = 15.0
    cfg.traffic.drop_end_s = 45.0
    cfg.traffic.rho = 0.30
    cfg.traffic.rho_min = 0.05
    cfg.traffic.rho_max = 0.80
    cfg.traffic.interval_s = 0.1
    cfg.traffic.low_s = 0.1
    cfg.traffic.high_s = 0.3
    cfg.traffic.increase = 0.05
    cfg.traffic.decrease = 0.5
    cfg.traffic.target_val_acc = 0.70

    cfg.spec = CN()
    cfg.spec.use = False
    cfg.spec.warm_round = 1
    cfg.spec.predict_freq = 1
    cfg.spec.hessian_approx_lambda = 1.0
    cfg.spec.normal_spec_lambda = 0.
    cfg.spec.do_compensation = True

    # for sample spec client
    cfg.spec.sample_client_num = -1
    cfg.spec.sample_client_rate = -1.0
    cfg.spec.sampler = 'optimal_all'
    cfg.spec.overselection = 1.3
    cfg.spec.recover_round = 100000
    cfg.spec.recover_eval_freq = 1

    # for predictor
    cfg.spec.predictor = CN()
    cfg.spec.predictor.type = 'default'
    cfg.spec.predictor.model_type = 'cv'
    cfg.spec.predictor.in_channel = 1
    cfg.spec.predictor.model_args = [{"s_hid_dim": 16,
                                      "t_hid_dim": 256,
                                      "s_block_num": 8,
                                      "t_block_num": 4}]
    # for predictor data
    cfg.spec.predictor.patch_size = 64
    cfg.spec.predictor.pre_seq_length = 5
    cfg.spec.predictor.aft_seq_length = 1
    cfg.spec.predictor.sum_seq_length = 1
    cfg.spec.predictor.train_seq_length = -1
    cfg.spec.start_spec_round = -1

    # for predictor trainer
    cfg.spec.predictor.device = 2
    cfg.spec.predictor.epochs = 2
    cfg.spec.predictor.topk = 0.2
    cfg.spec.predictor.train_size = 0.8
    cfg.spec.predictor.do_validation = True
    cfg.spec.predictor.criterion = [{'criterion_type': 'MSELoss'}]
    cfg.spec.predictor.optimizer = [{'type': 'Adam', 'lr': 3e-3, 'weight_decay': 0}]
    cfg.spec.predictor.scheduler = [{'type': 'StepLR', 'step_size': 100}]


    cfg.register_cfg_check_fun(assert_spec_cfg)
    cfg.register_cfg_check_fun(assert_traffic_cfg)


def assert_traffic_cfg(cfg):
    if cfg.traffic.enabled:
        assert cfg.federate.mode.lower() == 'standalone'
        assert cfg.federate.process_num <= 1
        assert cfg.federate.method.lower() in ('idpspec', 'fedavg')
        assert not (cfg.federate.online_aggr or cfg.asyn.use
                    or cfg.vertical.use or cfg.federate.use_ss)
        assert cfg.traffic.profile in ('H', 'X', 'D')
        assert cfg.traffic.shared_mbps in (10, 30, 100)
        assert 0 <= cfg.traffic.target_val_acc <= 1

def assert_spec_cfg(cfg):
    if not cfg.spec.use:
        return True
    assert cfg.spec.sampler in ['optimal_normal', 'optimal_all', 'random_normal', 'random_all']
    if cfg.spec.sample_client_num <= 0:
        if 0 < cfg.spec.sample_client_rate <= 1:
            spec_sample_num = cfg.federate.sample_client_num if 'normal' in cfg.spec.sampler else cfg.federate.client_num
            if spec_sample_num > 0:
                cfg.spec.sample_client_num = max(1, int(spec_sample_num * cfg.spec.sample_client_rate))
            else:
                cfg.spec.sample_client_num = 0
        else:
            cfg.spec.sample_client_num = 0

    if cfg.spec.predictor.train_seq_length > 0:
            train_seq_length = cfg.spec.predictor.pre_seq_length + cfg.spec.predictor.aft_seq_length
            assert cfg.spec.predictor.train_seq_length == train_seq_length, \
            "The length of the predictor training sequence must be equal to \
            the sum of the input sequence length and the output sequence length"
    else:
        cfg.spec.predictor.train_seq_length = \
        cfg.spec.predictor.pre_seq_length + cfg.spec.predictor.aft_seq_length

    if cfg.spec.start_spec_round > 0:
        start_spec_round = cfg.spec.predictor.train_seq_length + cfg.spec.warm_round 
        assert cfg.spec.start_spec_round >= start_spec_round, \
        "The number of rounds to start speculative execution must be greater than "+\
            "the sum of warm rounds and the training sequence length"
    else:
        cfg.spec.start_spec_round = cfg.spec.predictor.train_seq_length + cfg.spec.warm_round 
    
    pre_seq_length = cfg.spec.predictor.pre_seq_length // cfg.spec.predictor.sum_seq_length
    in_shape = {"grad_shape":[pre_seq_length,
                              cfg.spec.predictor.in_channel,
                              cfg.spec.predictor.patch_size,
                              cfg.spec.predictor.patch_size]}
    cfg.spec.predictor.model_args[0].update(in_shape)

    criterion_device = {'device': cfg.spec.predictor.device}
    cfg.spec.predictor.criterion[0].update(criterion_device)


register_config("spec", extend_spec_cfg)

